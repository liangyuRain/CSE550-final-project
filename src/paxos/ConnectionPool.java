package paxos;

import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.logging.Level;

public class ConnectionPool implements Closeable, AutoCloseable {

    public static final int CONNECTION_TIMEOUT = 500; // millisecond
    public static final int RECONNECT_INTERVAL = 500; // millisecond
    public static final int MIN_NUM_OF_CONNECTIONS = 10;
    public static final int MAX_NUM_OF_CONNECTIONS = MIN_NUM_OF_CONNECTIONS * 4;

    public static final int TEST_ALIVE_INTERVAL = 500; // millisecond
    public static final int TEST_ALIVE_TIMEOUT = 3 * TEST_ALIVE_INTERVAL;

    public static int readPort(Socket skt) throws IOException {
        int port = 0;
        InputStream input = skt.getInputStream();
        for (int i = 0; i < 2; ++i) {
            int b = input.read();
            if (b < 0) return -1;
            port = (port << 8) | b;
        }
        return port;
    }

    public static void writePort(Socket skt, int port) throws IOException {
        OutputStream output = skt.getOutputStream();
        output.write(port >> 8);
        output.write(port);
        output.flush();
    }

    private final ExecutorService executor;
    private final BiConsumer<Message, Address> messageHandler;
    private boolean closed = false;

    private final ArrayBlockingSetQueue<Package> outboundPackages;
    private final Address address;
    private final Address to;

    private final LogHandler logHandler;

    private final AtomicLong counter;
    private final ConcurrentHashMap<Long, Socket> connections;

    private final LongAdder outPkgCounter, inPkgCounter;

    public ConnectionPool(Address address,
                          Address to,
                          int packageQueueCapacity,
                          ExecutorService executor,
                          LogHandler logHandler,
                          BiConsumer<Message, Address> messageHandler) {
        this.logHandler = logHandler.derivative(String.format("[ConnectionPool %s]", to.hostname()));
        log(Level.FINEST, "Creating ConnectionPool");

        this.address = address;
        this.to = to;
        this.executor = executor;
        this.messageHandler = messageHandler;

        this.connections = new ConcurrentHashMap<>();
        this.counter = new AtomicLong();
        this.outPkgCounter = new LongAdder();
        this.inPkgCounter = new LongAdder();

        ArrayBlockingSetQueue<Package> outboundPackages = null;
        try {
            outboundPackages = new ArrayBlockingSetQueue<>(packageQueueCapacity,
                    ArrayBlockingSetQueue.FullQueuePolicy.DISCARD_OLDEST);
        } catch (Throwable e) {
            log(e);
            System.exit(1);
        }
        this.outboundPackages = outboundPackages;

        if (!to.equals(address)) {
            executor.execute(new ConnectionCreationTask());
        }

        log(Level.FINER, "Created ConnectionPool");
    }

    public ConnectionPoolStat getConnectionPoolStat() {
        Pair<Integer, Long> queueStat = outboundPackages.stat();
        return new ConnectionPoolStat(
                connections.size(),
                outboundPackages.size(),
                outPkgCounter.sum(),
                inPkgCounter.sum(),
                queueStat.getLeft(),
                queueStat.getRight(),
                System.nanoTime()
        );
    }

    @Override
    public void close() {
        try {
            log(Level.FINER, "Closing ConnectionPool");
            synchronized (connections) {
                closed = true;
                connections.notifyAll();
                outboundPackages.clear();
                for (Map.Entry<Long, Socket> entry : connections.entrySet()) {
                    long id = entry.getKey();
                    Socket skt = entry.getValue();
                    try {
                        skt.close();
                    } catch (IOException e) {
                        log(Level.SEVERE, String.format("Close connection %d to %s failed with %s", id, to, e));
                    }
                }
                connections.clear();
            }
        } catch (Throwable e) {
            log(e);
            System.exit(1);
        }
    }

    private class ConnectionCreationTask implements Runnable {

        @Override
        public void run() {
            try {
                for (; ; ) {
                    synchronized (connections) {
                        for (; ; ) {
                            int capacityRemain = outboundPackages.remainingCapacity();
                            int numOfConnection = connections.size();

                            if ((numOfConnection >= MIN_NUM_OF_CONNECTIONS && capacityRemain > 0) ||
                                    numOfConnection >= MAX_NUM_OF_CONNECTIONS) {
                                log(Level.FINEST, String.format(
                                        "Connection creation enters sleep, " +
                                                "num of connections: %d, " +
                                                "queue capacity remain: %d", numOfConnection, capacityRemain));
                                connections.wait();
                            } else {
                                log(Level.FINEST, String.format(
                                        "Connection creation awake, " +
                                                "num of connections: %d, " +
                                                "queue capacity remain: %d", numOfConnection, capacityRemain));
                                break;
                            }
                        }
                    }
                    if (closed) return;
                    try {
                        log(Level.FINEST, "Creating connection");
                        Socket skt = new Socket();
                        skt.connect(to.inetSocketAddress(), CONNECTION_TIMEOUT);
                        long id = addConnectionInternal(skt, true);
                        if (id >= 0) {
                            log(Level.FINER, String.format("Created connection %s to %s", id, to.hostname()));
                        } else {
                            skt.close();
                        }
                    } catch (SocketTimeoutException e) {
                        log(Level.SEVERE, String.format("Create connection to %s timed out", to.hostname()));
                    } catch (IOException e) {
                        log(Level.SEVERE, String.format("Create connection to %s failed with %s", to.hostname(), e));
                        Thread.sleep(RECONNECT_INTERVAL);
                    }
                }
            } catch (Throwable e) {
                log(e);
                System.exit(1);
            }
        }

    }

    public void addConnection(Socket skt) {
        try {
            long id = addConnectionInternal(skt, false);
            if (id >= 0) {
                log(Level.FINER, String.format("Added connection %d from %s", id, to.hostname()));
            } else {
                skt.close();
            }
        } catch (Throwable e) {
            log(e);
            System.exit(1);
        }
    }

    private long addConnectionInternal(Socket skt, boolean writePortFirst) {
        log(Level.FINEST, String.format("Adding connection %s", skt));
        if (to.equals(address)) {
            throw new IllegalStateException("Cannot add connection to self");
        }
        long id;
        synchronized (connections) {
            if (closed) {
                log(Level.SEVERE, "Connection addition failed: ConnectionPool closed");
                return -1;
            }
            if (connections.size() >= MAX_NUM_OF_CONNECTIONS) {
                log(Level.SEVERE, "Add connection failed due to too many connections");
                return -1;
            }
            id = counter.getAndIncrement();
            connections.put(id, skt);
            executor.execute(new SendTask(id, writePortFirst, logHandler));
            executor.execute(new ReceiveTask(id, logHandler));
        }
        log(Level.FINEST, String.format(
                "%s added as connection %d; Active connection to %s: %d",
                skt, id, to.hostname(), connections.size()));
        return id;
    }

    public void closeConnection(long id) {
        log(Level.FINEST, String.format("Closing connection %d", id));
        Socket skt = connections.remove(id);
        if (skt != null) {
            try {
                skt.close();
            } catch (IOException e) {
                log(Level.SEVERE, String.format("Close connection %d to %s failed with %s", id, to, e));
            }
            log(Level.FINER, String.format("Connection %d closed", id));
            synchronized (connections) {
                connections.notifyAll();
            }
        } else {
            log(Level.FINEST, String.format("Connection %d already closed", id));
        }
    }

    protected void log(Level level, String s) {
        logHandler.log(level, s);
    }

    protected void log(Throwable e) {
        logHandler.log(e);
    }

    public void send(Message message) {
        Package pkg = new Package(address, message.immutableCopy());
        log(Level.FINEST, String.format("Enqueuing package %s", pkg));
        if (to.equals(address)) {
            messageHandler.accept(pkg.message(), address);
        } else {
            Package ignored = outboundPackages.addWithDiscard(pkg);
            if (ignored != null) {
                log(Level.SEVERE, String.format(
                        "Package ignored because of full outbound package queue: %s", ignored));
            }
            if (outboundPackages.remainingCapacity() == 0) {
                synchronized (connections) {
                    connections.notifyAll();
                }
            }
            log(Level.FINEST, String.format("Enqueued package %s; Queue size: %d", pkg, outboundPackages.size()));
        }
    }

    private class SendTask implements Runnable {

        private final long id;
        private final boolean writePortFirst;
        private final LogHandler logHandler;

        public SendTask(long id, boolean writePortFirst, LogHandler logHandler) {
            this.id = id;
            this.writePortFirst = writePortFirst;
            this.logHandler = logHandler.derivative(String.format("[Connection %d] [Send]", id));
        }

        protected void log(Level level, String s) {
            logHandler.log(level, s);
        }

        protected void log(Throwable e) {
            logHandler.log(e);
        }

        @Override
        public void run() {
            try {
                Socket skt = connections.get(id);
                if (skt == null) return;
                try {
                    if (writePortFirst) writePort(skt, address.inetSocketAddress().getPort());
                    try (ObjectOutputStream sktOutput =
                                 new ObjectOutputStream(new BufferedOutputStream(skt.getOutputStream()))) {
                        for (; ; ) {
                            log(Level.FINEST, "Waiting for new package");
                            Package pkg = outboundPackages.poll(TEST_ALIVE_INTERVAL, TimeUnit.MILLISECONDS);
                            if (closed) return;
                            if (!connections.containsKey(id)) {
                                log(Level.SEVERE, "Connection has been closed");
                                return;
                            }
                            if (pkg != null) {
                                log(Level.FINEST, String.format(
                                        "Package dequeued; Queue size: %d; Dequeued package: %s",
                                        outboundPackages.size(), pkg));
                            } else {
                                pkg = new Package(address, new TestAlive(System.nanoTime()));
                            }
                            try {
                                sktOutput.writeObject(pkg);
                                sktOutput.reset();
                                sktOutput.flush();
                                log(pkg.message().logLevel(), String.format("Sent package %s", pkg));
                                if (!(pkg.message() instanceof TestAlive)) {
                                    outPkgCounter.increment();
                                }
                            } catch (IOException e) {
                                log(Level.SEVERE, String.format("Send failed with %s: %s", e, pkg));
                                return;
                            }
                        }
                    }
                } catch (IOException e) {
                    log(Level.SEVERE, String.format("Constructing ObjectOutputStream failed with %s", e));
                } finally {
                    closeConnection(id);
                }
            } catch (Throwable e) {
                log(e);
                System.exit(1);
            }
        }
    }

    private class ReceiveTask implements Runnable {

        private final long id;
        private final LogHandler logHandler;

        public ReceiveTask(long id, LogHandler logHandler) {
            this.id = id;
            this.logHandler = logHandler.derivative(String.format("[Connection %d] [Receive]", id));
        }

        protected void log(Level level, String s) {
            logHandler.log(level, s);
        }

        protected void log(Throwable e) {
            logHandler.log(e);
        }

        @Override
        public void run() {
            try {
                Socket skt = connections.get(id);
                if (skt == null) return;
                try {
                    try {
                        skt.setSoTimeout(TEST_ALIVE_TIMEOUT);
                    } catch (SocketException e) {
                        log(Level.SEVERE, String.format("%s setSoTimeout failed", skt));
                        return;
                    }
                    try (ObjectInputStream sktInput =
                                 new ObjectInputStream(new BufferedInputStream(skt.getInputStream()))) {
                        for (; ; ) {
                            Package pkg;
                            try {
                                pkg = (Package) sktInput.readObject();
                            } catch (ClassNotFoundException | IOException e) {  // Incomplete package?
                                log(Level.SEVERE, String.format("Receive failed with %s", e));
                                return;
                            }
                            if (closed) return;
                            Message message = pkg.message();
                            Address sender = pkg.sender();
                            log(message.logLevel(), String.format("Got package from %s: %s", sender.hostname(), pkg));
                            if (!(message instanceof TestAlive)) {
                                messageHandler.accept(message, sender);
                                inPkgCounter.increment();
                            }
                            if (!connections.containsKey(id)) {
                                log(Level.SEVERE, "Connection has been closed");
                                return;
                            }
                        }
                    } catch (IOException e) {
                        log(Level.SEVERE, String.format("Constructing ObjectInputStream failed with %s", e));
                    }
                } finally {
                    closeConnection(id);
                }
            } catch (Throwable e) {
                log(e);
                System.exit(1);
            }
        }

    }

    @Data
    public static class ConnectionPoolStat {
        public final int numOfConnections, packageQueueSize;
        public final long outPkgCounter, inPkgCounter;
        public final int queueCount;
        public final long queueTotalDelay;
        public final long timestamp;
    }

}

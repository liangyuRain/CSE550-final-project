package paxos;

import lombok.extern.java.Log;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;

@Log
public class Node {

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT %1$tL] %5$s %n");
    }

    private static final int LOG_FILE_SIZE_LIMIT = Integer.MAX_VALUE;
    private static final int LOG_FILE_COUNT = 1;

    private final Address address;

    private final ScheduledThreadPoolExecutor scheduledExecutor;
    private final ThreadPoolExecutor dynamicExecutor;

    private Level logLevel;

    private final ConcurrentHashMap<Address, ConnectionPool> addrToConn;

    public Node(Address address) throws IOException {
        this.address = address;
        this.logLevel = Level.ALL;
        this.addrToConn = new ConcurrentHashMap<>();

        FileHandler fh = new FileHandler(String.format("%s.log", this.getClass().getSimpleName()),
                LOG_FILE_SIZE_LIMIT, LOG_FILE_COUNT);
        fh.setFormatter(new SimpleFormatter());
        LOG.addHandler(fh);

        this.scheduledExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(10);
        this.dynamicExecutor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        Function<ThreadPoolExecutor, Runnable> logThreadPool = executor ->
                () -> log(Level.FINEST, String.format(
                        "%s info: Queue size: %d, Num of active thread: %d, Pool size: %d, Core pool size: %d",
                        executor.getClass().getSimpleName(),
                        executor.getQueue().size(),
                        executor.getActiveCount(),
                        executor.getPoolSize(),
                        executor.getCorePoolSize()));
        this.scheduledExecutor.scheduleWithFixedDelay(
                logThreadPool.apply(scheduledExecutor), 0, 10, TimeUnit.SECONDS);
        this.scheduledExecutor.scheduleWithFixedDelay(
                logThreadPool.apply(dynamicExecutor), 0, 10, TimeUnit.SECONDS);
    }

    void init() {
    }

    void listen() {
        try {
            ServerSocket serverSkt = new ServerSocket(Address.PORT);
            for (; ; ) {
                try {
                    Socket clientSocket = serverSkt.accept();
                    String clientHostname = ((InetSocketAddress) clientSocket.getRemoteSocketAddress()).getHostName();
                    Address clientAddr = new Address(clientHostname);
                    log(Level.FINEST, String.format(
                            "Accepted connection from %s: %s", clientAddr.hostname(), clientSocket));
                    dynamicExecutor.execute(() -> addrToConn
                            .computeIfAbsent(clientAddr, ConnectionPool::new)
                            .addConnection(clientSocket));
                } catch (IOException e) {
                    log(Level.SEVERE, String.format("Listening failed with %s", e));
                }
            }
        } catch (IOException e) {
            log(e);
            System.exit(1);
        }
    }

    public final Address address() {
        return address;
    }

    public void setLogLevel(Level level) {
        this.logLevel = level;
    }

    protected void log(Level level, String s) {
        if (level.intValue() >= this.logLevel.intValue()) {
            LOG.info(String.format("[Node %s] %s", address().hostname(), s));
        }
    }

    protected void log(Throwable e, String prefix) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        sw.write(prefix);
        e.printStackTrace(pw);
        pw.write(' ');
        pw.write(e.toString());
        pw.flush();
        this.log(Level.SEVERE, sw.toString());
        pw.close();
    }

    protected void log(Throwable e) {
        this.log(e, "");
    }

    protected void send(Message message, Address to) {
        addrToConn.computeIfAbsent(to, ConnectionPool::new).send(message);
    }

    protected void broadcast(Message message, Collection<Address> to) {
        broadcast(message, to.toArray(new Address[0]));
    }

    protected void broadcast(Message message, Address[] to) {
        if (to.length > 0) {
            log(message.logLevel(), String.format("Broadcast %s to %s", message, Arrays.toString(to)));
            for (Address addr : to) {
                send(message, addr);
            }
        }
    }

    protected void set(Timeout timeout) {
        schedule(new TimeoutTask(timeout), timeout.timeoutLengthMillis());
        log(timeout.logLevel(), String.format("Timeout %s set", timeout));
    }

    private void schedule(Runnable command, long delay) {
        scheduledExecutor.schedule(() -> {
            try {
                dynamicExecutor.execute(command);
            } catch (Throwable e) {
                log(e);
                System.exit(1);
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private class TimeoutTask implements Runnable {

        private final Timeout timeout;

        public TimeoutTask(Timeout timeout) {
            this.timeout = timeout;
        }

        @Override
        public void run() {
            try {
                log(timeout.logLevel(), String.format("Timeout %s triggered", timeout));
                Class<?> timeout_class = timeout.getClass();
                try {
                    Method method = Node.this.getClass().getDeclaredMethod(
                            "on" + timeout_class.getSimpleName(), timeout_class);
                    method.setAccessible(true);
                    method.invoke(Node.this, timeout);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            } catch (Throwable e) {
                log(e);
                System.exit(1);
            }
        }

    }

    private class ConnectionPool {

        public static final int CONNECTION_TIMEOUT = 500; // millisecond
        public static final int RECONNECT_INTERVAL = 100; // millisecond
        public static final int MESSAGE_QUEUE_CAPACITY = 10;
        public static final int MAX_NUM_OF_CONNECTIONS = 10;

        public static final int TEST_ALIVE_INTERVAL = 100; // millisecond
        public static final int TEST_ALIVE_TIMEOUT = 3 * TEST_ALIVE_INTERVAL;

        private final BlockingQueue<Pair<Package, Long>> outboundPackages;
        private final Address to;

        private final AtomicLong counter;
        private final ConcurrentHashMap<Long, Socket> connections;
        private final ConcurrentHashMap<Long, Long> lastReceival;

        public ConnectionPool(Address to) {
            this.outboundPackages = new ArrayBlockingQueue<>(MESSAGE_QUEUE_CAPACITY);
            this.to = to;
            this.connections = new ConcurrentHashMap<>();
            this.lastReceival = new ConcurrentHashMap<>();
            this.counter = new AtomicLong(0);

            if (!to.equals(Node.this.address())) {
                dynamicExecutor.execute(new ConnectionCreationTask());
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

                                if ((numOfConnection >= 2 && capacityRemain > 0) ||
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

                        try {
                            log(Level.FINEST, "Creating connection");
                            Socket skt = new Socket();
                            skt.connect(to.inetSocketAddress(), CONNECTION_TIMEOUT);
                            long id = addConnectionInternal(skt);
                            if (id >= 0) {
                                log(Level.FINER, String.format("Created connection %s to %s", id, to.hostname()));
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
                long id = addConnectionInternal(skt);
                if (id >= 0) {
                    log(Level.FINER, String.format("Added connection %d from %s", id, to.hostname()));
                }
            } catch (Throwable e) {
                log(e);
                System.exit(1);
            }
        }

        private long addConnectionInternal(Socket skt) {
            log(Level.FINEST, String.format("Adding connection %s", skt));
            if (to.equals(Node.this.address())) {
                throw new IllegalStateException("Cannot add connection to self");
            }
            long id;
            synchronized (connections) {
                if (connections.size() >= MAX_NUM_OF_CONNECTIONS) {
                    log(Level.SEVERE, "Add connection failed due to too many connections");
                    return -1;
                }
                id = counter.getAndIncrement();
                connections.put(id, skt);
            }
            lastReceival.put(id, System.currentTimeMillis());
            dynamicExecutor.execute(new SendTask(id));
            dynamicExecutor.execute(new ReceiveTask(id));
            log(Level.FINEST, String.format(
                    "%s added as connection %d; Active connection to %s: %d",
                    skt, id, to.hostname(), connections.size()));
            return id;
        }

        public void closeConnection(long id) {
            log(Level.FINEST, String.format("Closing connection %d", id));
            lastReceival.remove(id);
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
            Node.this.log(level, String.format("[ConnectionPool %s] %s", to.hostname(), s));
        }

        protected void log(Throwable e, String prefix) {
            Node.this.log(e, String.format("[ConnectionPool %s] %s", to.hostname(), prefix));
        }

        protected void log(Throwable e) {
            this.log(e, "");
        }

        public void send(Message message) {
            Package pkg = new Package(Node.this.address, message.immutableCopy());
            log(Level.FINEST, String.format("Enqueuing package %s", pkg));
            if (to.equals(Node.this.address())) {
                dynamicExecutor.execute(() -> handleMessage(pkg.message(), Node.this.address()));
            } else {
                if (!outboundPackages.offer(Pair.of(pkg, System.nanoTime()))) {
                    log(Level.SEVERE, String.format("Package ignored because of full outbound package queue: %s", pkg));
                    synchronized (connections) {
                        connections.notifyAll();
                    }
                } else {
                    log(Level.FINEST, String.format(
                            "Enqueued package %s; Queue size: %d", pkg, outboundPackages.size()));
                }
            }
        }

        private void handleMessage(Message message, Address sender) {
            try {
                Class<?> messageClass = message.getClass();
                try {
                    Method method = Node.this.getClass().getDeclaredMethod(
                            "handle" + messageClass.getSimpleName(), messageClass, Address.class);
                    method.setAccessible(true);
                    method.invoke(Node.this, message, sender);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            } catch (Throwable e) {
                log(e);
                System.exit(1);
            }
        }

        private class SendTask implements Runnable {

            private final long id;

            public SendTask(long id) {
                this.id = id;
            }

            protected void log(Level level, String s) {
                ConnectionPool.this.log(level, String.format("[Connection %d] %s", id, s));
            }

            protected void log(Throwable e, String prefix) {
                ConnectionPool.this.log(e, String.format("[Connection %d] %s", id, prefix));
            }

            protected void log(Throwable e) {
                this.log(e, "");
            }

            @Override
            public void run() {
                try {
                    Socket skt = connections.get(id);
                    if (skt == null) return;
                    ObjectOutputStream sktOutput;
                    try {
                        sktOutput = new ObjectOutputStream(skt.getOutputStream());
                    } catch (IOException e) {
                        log(Level.SEVERE, String.format("Constructing ObjectOutputStream failed with %s", e));
                        closeConnection(id);
                        return;
                    }
                    for (; ; ) {
                        log(Level.FINEST, "Waiting for new package");
                        Pair<Package, Long> item = outboundPackages.poll(TEST_ALIVE_INTERVAL, TimeUnit.MILLISECONDS);
                        if (!connections.containsKey(id)) {
                            log(Level.SEVERE, "Connection has been closed");
                            return;
                        }
                        if (System.currentTimeMillis() - lastReceival.get(id) > TEST_ALIVE_TIMEOUT) {
                            log(Level.SEVERE, "Connection not alive");
                            try {
                                sktOutput.close();
                            } catch (IOException ignored) {
                            }
                            closeConnection(id);
                            return;
                        }
                        Package pkg;
                        if (item != null) {
                            long dequeueTimestamp = System.nanoTime();
                            long enqueueTimestamp = item.getRight();
                            pkg = item.getLeft();
                            log(Level.FINEST, String.format(
                                    "Package dequeued; Queue size: %d; Queue delay: %.3f us; Dequeued package: %s",
                                    outboundPackages.size(), (dequeueTimestamp - enqueueTimestamp) / 1.0e3, pkg));
                        } else {
                            pkg = new Package(Node.this.address, new TestAlive(System.currentTimeMillis()));
                        }
                        try {
                            sktOutput.writeObject(pkg);
                            sktOutput.reset();
                            log(pkg.message().logLevel(), String.format("Sent package %s", pkg));
                        } catch (IOException e) {
                            log(Level.SEVERE, String.format("Send failed with %s: %s", e, pkg));
                            try {
                                sktOutput.close();
                            } catch (IOException ignored) {
                            }
                            closeConnection(id);
                            return;
                        }
                    }
                } catch (Throwable e) {
                    log(e);
                    System.exit(1);
                }
            }
        }

        private class ReceiveTask implements Runnable {

            private final long id;

            public ReceiveTask(long id) {
                this.id = id;
            }

            protected void log(Level level, String s) {
                ConnectionPool.this.log(level, String.format("[Connection %d] %s", id, s));
            }

            protected void log(Throwable e, String prefix) {
                ConnectionPool.this.log(e, String.format("[Connection %d] %s", id, prefix));
            }

            protected void log(Throwable e) {
                this.log(e, "");
            }

            @Override
            public void run() {
                try {
                    Socket skt = connections.get(id);
                    if (skt == null) return;
                    ObjectInputStream sktInput;
                    try {
                        sktInput = new ObjectInputStream(skt.getInputStream());
                    } catch (IOException e) {
                        log(Level.SEVERE, String.format("Constructing ObjectInputStream failed with %s", e));
                        closeConnection(id);
                        return;
                    }
                    for (; ; ) {
                        Package pkg;
                        try {
                            pkg = (Package) sktInput.readObject();
                        } catch (ClassNotFoundException | IOException e) {  // Incomplete package?
                            log(Level.SEVERE, String.format("Receive failed with %s", e));
                            try {
                                sktInput.close();
                            } catch (IOException ignored) {
                            }
                            closeConnection(id);
                            return;
                        }
                        lastReceival.put(id, System.currentTimeMillis());
                        Message message = pkg.message();
                        Address sender = pkg.sender();
                        log(message.logLevel(), String.format("Got package from %s: %s", sender.hostname(), pkg));
                        if (!(message instanceof TestAlive)) {
                            dynamicExecutor.execute(() -> handleMessage(message, sender));
                        }
                        if (!connections.containsKey(id)) {
                            log(Level.SEVERE, "Connection has been closed");
                            return;
                        }
                    }
                } catch (Throwable e) {
                    log(e);
                    System.exit(1);
                }
            }

        }

    }

}

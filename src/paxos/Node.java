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
        this.scheduledExecutor.scheduleWithFixedDelay(
                () -> log(Level.FINEST, String.format(
                        "ScheduledThreadPoolExecutor info: " +
                                "Queue size: %d, Num of active thread: %d, Pool size: %d, Core pool size: %d",
                        scheduledExecutor.getQueue().size(),
                        scheduledExecutor.getActiveCount(),
                        scheduledExecutor.getPoolSize(),
                        scheduledExecutor.getCorePoolSize())),
                0, 10, TimeUnit.SECONDS
        );
        this.scheduledExecutor.scheduleWithFixedDelay(
                () -> log(Level.FINEST, String.format(
                        "ThreadPoolExecutor info: " +
                                "Queue size: %d, Num of active thread: %d, Pool size: %d, Core pool size: %d",
                        dynamicExecutor.getQueue().size(),
                        dynamicExecutor.getActiveCount(),
                        dynamicExecutor.getPoolSize(),
                        dynamicExecutor.getCorePoolSize())),
                0, 10, TimeUnit.SECONDS
        );
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

        public static final int CONNECTION_TIMEOUT = 3000;
        public static final int MESSAGE_QUEUE_CAPACITY = 16;
        public static final int MAX_NUM_OF_CONNECTIONS = 10;

        private final BlockingQueue<Pair<byte[], Long>> outboundPackages;
        private final Address to;

        private final AtomicLong counter;
        private final ConcurrentHashMap<Long, Socket> connections;

        public ConnectionPool(Address to) {
            this.outboundPackages = new ArrayBlockingQueue<>(MESSAGE_QUEUE_CAPACITY);
            this.to = to;
            this.connections = new ConcurrentHashMap<>();
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
                            Thread.sleep(CONNECTION_TIMEOUT);
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
            dynamicExecutor.execute(new SendTask(id));
            dynamicExecutor.execute(new ReceiveTask(id));
            log(Level.FINEST, String.format(
                    "%s added as connection %d; Active connection to %s: %d",
                    skt, id, to.hostname(), connections.size()));
            return id;
        }

        public void closeConnection(long id) {
            log(Level.FINEST, String.format("Closing connection %d", id));
            Socket skt = connections.get(id);
            if (skt != null) {
                try {
                    skt.close();
                } catch (IOException e) {
                    log(Level.SEVERE, String.format("Close connection %d to %s failed with %s", id, to, e));
                }
                connections.remove(id);
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
            log(Level.FINEST, String.format("Enqueuing message %s", message));
            try {
                ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
                ObjectOutputStream objOutput = new ObjectOutputStream(byteOutput);

                objOutput.writeObject(new Package(Node.this.address, message));
                objOutput.flush();
                objOutput.close();
                byte[] bytes = byteOutput.toByteArray();
                if (to.equals(Node.this.address())) {
                    Message copy = deserialize(bytes).message();
                    dynamicExecutor.execute(() -> handleMessage(copy, Node.this.address()));
                } else {
                    if (!outboundPackages.offer(Pair.of(bytes, System.nanoTime()))) {
                        log(Level.SEVERE, String.format(
                                "Message ignored because of full outbound package queue: %s", message));
                        synchronized (connections) {
                            connections.notifyAll();
                        }
                    } else {
                        log(Level.FINEST, String.format(
                                "Enqueued message %s; Queue size: %d", message, outboundPackages.size()));
                    }
                }
            } catch (Throwable e) {
                log(e);
                System.exit(1);
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

        private Package deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
            ObjectInputStream objInput = new ObjectInputStream(new ByteArrayInputStream(bytes));
            Package pkg = (Package) objInput.readObject();
            objInput.close();
            return pkg;
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
                        Pair<byte[], Long> item;
                        do {
                            item = outboundPackages.poll(10, TimeUnit.SECONDS);
                            if (!connections.containsKey(id)) {
                                log(Level.SEVERE, "Connection has been closed");
                                return;
                            }
                        } while (item == null);
                        long dequeueTimestamp = System.nanoTime();
                        byte[] bytes = item.getLeft();
                        long enqueueTimestamp = item.getRight();
                        Package pkg = deserialize(bytes);
                        log(Level.FINEST, String.format(
                                "Message dequeued; Queue size: %d; Queue delay: %.3f us; Dequeued message: %s",
                                outboundPackages.size(), (dequeueTimestamp - enqueueTimestamp) / 1.0e3, pkg.message()));
                        try {
                            sktOutput.writeObject(pkg);
                            sktOutput.reset();
                            log(pkg.message().logLevel(), String.format("Sent message %s", pkg.message()));
                        } catch (IOException e) {
                            log(Level.SEVERE, String.format("Send failed with %s: %s", e, pkg.message()));
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
                        Message message = pkg.message();
                        Address sender = pkg.sender();
                        log(message.logLevel(), String.format("Got message from %s: %s ", sender.hostname(), message));
                        dynamicExecutor.execute(() -> handleMessage(message, sender));
                    }
                } catch (Throwable e) {
                    log(e);
                    System.exit(1);
                }
            }

        }

    }

}

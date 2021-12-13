package paxos;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class Node {

    public static final int PERODIC_LOG_INTERVAL = 1000;
    public static final long CONNECTIONPOOL_TIMEOUT = 5 * 1000; // millisecond
    public static final long CONNECTIONPOOL_GC_INTERVAL = 1000; // millisecond

    private final Address address;
    private final int packageQueueCapacity;

    private final ScheduledExecutorService scheduledExecutor;
    private final ExecutorService dynamicExecutor;
    protected final ThreadPoolExecutor messageHandlerExecutor;

    private final LogHandler logHandler;

    private final ConcurrentHashMap<Address, ConnectionPool> addrToConn;
    private final ConcurrentHashMap<Address, Long> lastActivity;

    protected boolean PRINT_LOG_ESSENTIAL() {
        return true;
    }

    public Node(Address address,
                ThreadPoolExecutor messageHandlerExecutor,
                int packageQueueCapacity) throws IOException {
        this.address = address;
        this.packageQueueCapacity = packageQueueCapacity;
        this.logHandler = new LogHandler(String.format("[Node %s]", address.hostname()));
        this.logHandler.addFile(String.format(
                "%s_%s.log", this.getClass().getSimpleName(), address.hostname().replaceAll("[.:]", "_")));
        this.addrToConn = new ConcurrentHashMap<>();
        this.lastActivity = new ConcurrentHashMap<>();

        this.scheduledExecutor = Executors.newScheduledThreadPool(5);
        this.dynamicExecutor = Executors.newCachedThreadPool();
        this.messageHandlerExecutor = messageHandlerExecutor;

        this.scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                dynamicExecutor.execute(this::periodicLog);
            } catch (Throwable e) {
                log(e);
                System.exit(1);
            }
        }, 0, PERODIC_LOG_INTERVAL, TimeUnit.MILLISECONDS);
        this.scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                dynamicExecutor.execute(this::connectionPoolGC);
            } catch (Throwable e) {
                log(e);
                System.exit(1);
            }
        }, 0, CONNECTIONPOOL_GC_INTERVAL, TimeUnit.MILLISECONDS);
    }

    public void init() {
    }

    public final void listen() {
        try {
            ServerSocket serverSkt = new ServerSocket(address.inetSocketAddress().getPort());
            for (; ; ) {
                try {
                    Socket clientSocket = serverSkt.accept();
                    dynamicExecutor.execute(() -> addSocket(clientSocket));
                } catch (IOException e) {
                    log(Level.SEVERE, String.format("Listening failed with %s", e));
                }
            }
        } catch (Throwable e) {
            log(e);
            System.exit(1);
        }
    }

    private void addSocket(Socket clientSocket) {
        try {
            clientSocket.setSoTimeout(60 * 1000);
            int port = ConnectionPool.readPort(clientSocket);
            if (port < 0) {
                log(Level.SEVERE, "Failed to read port number");
                return;
            }
            Address clientAddr = new Address(
                    ((InetSocketAddress) clientSocket.getRemoteSocketAddress())
                            .getAddress().getHostAddress(),
                    port);
            log(Level.FINEST, String.format(
                    "Accepted connection from %s: %s", clientAddr.hostname(), clientSocket));
            addrToConn.computeIfAbsent(clientAddr, k ->
                            new ConnectionPool(
                                    address,
                                    k,
                                    packageQueueCapacity,
                                    logHandler,
                                    this::handleMessage))
                    .addConnection(clientSocket);
            lastActivity.compute(clientAddr,
                    (k, t) -> Math.max(System.currentTimeMillis(), t == null ? Integer.MIN_VALUE : t));
        } catch (IOException e) {
            log(Level.SEVERE, String.format("Failed to add socket: %s", e));
        } catch (Throwable e) {
            log(e);
            System.exit(1);
        }
    }

    public final Address address() {
        return address;
    }

    public void setLogLevel(Level level) {
        LogHandler.logLevel = level;
    }

    protected void log(Level level, String s) {
        logHandler.log(level, s);
    }

    protected void log(Throwable e) {
        logHandler.log(e);
    }

    protected final void send(Message message, Address to) {
        addrToConn.computeIfAbsent(to, k -> new ConnectionPool(
                address, k, packageQueueCapacity, logHandler, this::handleMessage)).send(message);
        lastActivity.compute(to, (k, t) -> Math.max(System.currentTimeMillis(), t == null ? Integer.MIN_VALUE : t));
    }

    protected final void broadcast(Message message, Collection<Address> to) {
        broadcast(message, to.toArray(new Address[0]));
    }

    protected final void broadcast(Message message, Address[] to) {
        if (to.length > 0) {
            log(message.logLevel(), String.format("Broadcast %s to %s", message, Arrays.toString(to)));
            Message copy = message.immutableCopy();
            for (Address addr : to) {
                send(copy, addr);
            }
        } else {
            log(Level.FINEST, String.format("Broadcast ignored because of empty destination: %s", message));
        }
    }

    protected final void set(Timeout timeout) {
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

    @AllArgsConstructor
    private class TimeoutTask implements Runnable {

        private final Timeout timeout;

        @Override
        public void run() {
            log(timeout.logLevel(), String.format("Timeout triggered: %s ", timeout));
            long start = System.nanoTime();
            try {
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
            log(timeout.logLevel(), String.format(
                    "Timeout handled with %.3f us: %s", (System.nanoTime() - start) / 1.0e3, timeout));
        }

    }

    protected boolean messageFilter(Message message, Address sender) {
        return true;
    }

    private void handleMessage(Message message, Address sender) {
        if (messageFilter(message, sender)) {
            messageHandlerExecutor.execute(new MessageHandlerTask(message, sender));
        } else {
            log(Level.FINEST, String.format("Message from %s has been filtered out: %s", sender, message));
        }
        lastActivity.compute(sender, (k, t) -> Math.max(System.currentTimeMillis(), t == null ? Integer.MIN_VALUE : t));
    }

    @AllArgsConstructor
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private class MessageHandlerTask implements Runnable {

        @EqualsAndHashCode.Include
        private final Message message;
        @EqualsAndHashCode.Include
        private final Address sender;

        @Override
        public void run() {
            log(Level.FINEST, String.format("Handling message from %s: %s", sender, message));
            long start = System.nanoTime();
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
                log(Level.SEVERE, String.format(
                        "Fatal error encountered while handling message from %s: %s", sender, message));
                log(e);
                System.exit(1);
            }
            log(Level.FINEST, String.format(
                    "Handled message from %s with %.3f us: %s", sender, (System.nanoTime() - start) / 1.0e3, message));
        }

    }

    private void connectionPoolGC() {
        try {
            long now = System.currentTimeMillis();
            for (Map.Entry<Address, Long> entry : lastActivity.entrySet()) {
                if (now - entry.getValue() > CONNECTIONPOOL_TIMEOUT) {
                    ConnectionPool cp = addrToConn.remove(entry.getKey());
                    if (cp != null) dynamicExecutor.execute(cp::close);
                }
            }
        } catch (Throwable e) {
            log(e);
            System.exit(1);
        }
    }

    private void periodicLog() {
        String padding = StringUtils.repeat(System.lineSeparator(), 100);
        try {
            StringBuilder sb = new StringBuilder(padding);

            logEssential(sb);

            if (sb.length() > 0) {
                String s = sb.toString();
                log(Level.INFO, s);
                if (PRINT_LOG_ESSENTIAL()) {
                    System.out.println(s);
                    System.out.println();
                }
            }
        } catch (Throwable e) {
            log(e);
            System.exit(1);
        }
    }

    private final HashMap<Address, ConnectionPool.ConnectionPoolStat> lastConnPoolStats = new HashMap<>();

    private static final BiFunction<ThreadPoolExecutor, String, String> logThreadPool = (executor, info) ->
            String.format(
                    "%s info: Queue size: %d, Num of active thread: %d, Pool size: %d, Core pool size: %d",
                    info,
                    executor.getQueue().size(),
                    executor.getActiveCount(),
                    executor.getPoolSize(),
                    executor.getCorePoolSize());

    protected void logEssential(StringBuilder sb) {
        double totalOutThroughput = 0.0;
        double totalInThroughput = 0.0;
        long totalQueueDelay = 0;
        long totalQueueCount = 0;

        sb.append(String.format("[%s] [%s] Log Essential:",
                LogHandler.TIME_FORMATTER.format(LocalDateTime.now()), address.hostname()));
        sb.append(System.lineSeparator());

        lastConnPoolStats.keySet().removeIf(((Predicate<Address>) addrToConn::containsKey).negate());

        for (Map.Entry<Address, ConnectionPool> entry :
                addrToConn.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .collect(Collectors.toList())) {
            Address addr = entry.getKey();
            ConnectionPool.ConnectionPoolStat connPoolStat = entry.getValue().getConnectionPoolStat();
            sb.append(String.format("(%s): Num of conn: %d", addr.hostname(), connPoolStat.numOfConnections));

            ConnectionPool.ConnectionPoolStat lastRecord = lastConnPoolStats.get(addr);
            int lastQueueCount = 0;
            long lastQueueTotalDelay = 0;
            if (lastRecord != null) {
                lastQueueCount = lastRecord.queueCount;
                lastQueueTotalDelay = lastRecord.queueTotalDelay;

                double outThroughput = (connPoolStat.outPkgCounter - lastRecord.outPkgCounter) * 1.0e9 /
                        (connPoolStat.timestamp - lastRecord.timestamp);
                double inThroughput = (connPoolStat.inPkgCounter - lastRecord.inPkgCounter) * 1.0e9 /
                        (connPoolStat.timestamp - lastRecord.timestamp);
                sb.append(String.format(
                        ", Send throughput: %.3f, Recv throughput: %.3f", outThroughput, inThroughput));

                totalOutThroughput += outThroughput;
                totalInThroughput += inThroughput;
            }
            double averageQueueDelay = (connPoolStat.queueTotalDelay - lastQueueTotalDelay) / 1.0e3 /
                    (connPoolStat.queueCount - lastQueueCount);
            sb.append(String.format(", Avg queue delay: %.3f us", averageQueueDelay));

            totalQueueDelay += connPoolStat.queueTotalDelay - lastQueueTotalDelay;
            totalQueueCount += connPoolStat.queueCount - lastQueueCount;

            lastConnPoolStats.put(addr, connPoolStat);

            sb.append(System.lineSeparator());
        }
        sb.append(String.format(
                "Total send throughput: %.3f, Total recv throughput: %.3f, " +
                        "Overall avg queue delay: %.3f us",
                totalOutThroughput, totalInThroughput, totalQueueDelay / 1.0e3 / totalQueueCount));
        sb.append(System.lineSeparator());
        sb.append(System.lineSeparator());

        sb.append(logThreadPool.apply(messageHandlerExecutor, "MessageHandlerExecutor"));
    }

}

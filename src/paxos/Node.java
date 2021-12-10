package paxos;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class Node {

    private final Address address;
    private final int packageQueueCapacity;

    private final ScheduledThreadPoolExecutor scheduledExecutor;
    private final ThreadPoolExecutor dynamicExecutor;
    protected final ThreadPoolExecutor messageHandlerExecutor;

    private final LogHandler logHandler;

    private final ConcurrentHashMap<Address, ConnectionPool> addrToConn;

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

        this.scheduledExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(10);
        this.dynamicExecutor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        this.messageHandlerExecutor = messageHandlerExecutor;

        this.dynamicExecutor.execute(this::logEssential);
    }

    public void init() {
    }

    public final void listen() {
        try {
            ServerSocket serverSkt = new ServerSocket(address.inetSocketAddress().getPort());
            for (; ; ) {
                try {
                    Socket clientSocket = serverSkt.accept();
                    dynamicExecutor.execute(() -> {
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
                                                    dynamicExecutor,
                                                    this::handleMessage))
                                    .addConnection(clientSocket);
                        } catch (Throwable e) {
                            log(e);
                            System.exit(1);
                        }
                    });
                } catch (IOException e) {
                    log(Level.SEVERE, String.format("Listening failed with %s", e));
                }
            }
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
                address, k, packageQueueCapacity, logHandler, dynamicExecutor, this::handleMessage)).send(message);
    }

    protected final void broadcast(Message message, Collection<Address> to) {
        broadcast(message, to.toArray(new Address[0]));
    }

    protected final void broadcast(Message message, Address[] to) {
        if (to.length > 0) {
            log(message.logLevel(), String.format("Broadcast %s to %s", message, Arrays.toString(to)));
            for (Address addr : to) {
                send(message, addr);
            }
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

    private void logEssential() {
        try {
            BiFunction<ThreadPoolExecutor, String, String> logThreadPool = (executor, info) ->
                    String.format(
                            "%s info: Queue size: %d, Num of active thread: %d, Pool size: %d, Core pool size: %d",
                            info,
                            executor.getQueue().size(),
                            executor.getActiveCount(),
                            executor.getPoolSize(),
                            executor.getCorePoolSize());
            HashMap<Address, ConnectionPool.ConnectionPoolStat> lastConnPoolStats = new HashMap<>();
            for (; ; ) {
                double totalOutThroughput = 0.0;
                double totalInThroughput = 0.0;
                long totalQueueDelay = 0;
                long totalQueueCount = 0;

                StringBuilder sb = new StringBuilder();
                sb.append(String.format("[%s] Log Essential:", address.hostname()));
                sb.append(System.lineSeparator());
                for (Map.Entry<Address, ConnectionPool> entry :
                        addrToConn.entrySet().stream()
                                .sorted(Map.Entry.comparingByKey())
                                .collect(Collectors.toList())) {
                    Address addr = entry.getKey();
                    ConnectionPool.ConnectionPoolStat connPoolStat = entry.getValue().getConnectionPoolStat();
                    sb.append(String.format(
                            "Address: %s, Num of connections: %d, PackageQueue size: %d",
                            addr, connPoolStat.numOfConnections, connPoolStat.packageQueueSize));

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
                                ", Send throughput: %.3f, Receive throughput: %.3f", outThroughput, inThroughput));

                        totalOutThroughput += outThroughput;
                        totalInThroughput += inThroughput;
                    }
                    double averageQueueDelay = (connPoolStat.queueTotalDelay - lastQueueTotalDelay) / 1.0e3 /
                            (connPoolStat.queueCount - lastQueueCount);
                    sb.append(String.format(", Average queue delay: %.3f us", averageQueueDelay));

                    totalQueueDelay += connPoolStat.queueTotalDelay - lastQueueTotalDelay;
                    totalQueueCount += connPoolStat.queueCount - lastQueueCount;

                    lastConnPoolStats.put(addr, connPoolStat);

                    sb.append(System.lineSeparator());
                }
                sb.append(String.format(
                        "Total send throughput: %.3f, Total receive throughput: %.3f, " +
                                "Overall average queue delay: %.3f us",
                        totalOutThroughput, totalInThroughput, totalQueueDelay / 1.0e3 / totalQueueCount));
                sb.append(System.lineSeparator());

                sb.append(logThreadPool.apply(scheduledExecutor, "ScheduledExecutor"));
                sb.append(System.lineSeparator());
                sb.append(logThreadPool.apply(dynamicExecutor, "DynamicExecutor"));
                sb.append(System.lineSeparator());
                sb.append(logThreadPool.apply(messageHandlerExecutor, "MessageHandlerExecutor"));

                String s = sb.toString();
                log(Level.FINEST, s);
                if (PRINT_LOG_ESSENTIAL()) {
                    System.out.println(s);
                    System.out.println();
                }

                Thread.sleep(1000);
            }
        } catch (Throwable e) {
            log(e);
            System.exit(1);
        }
    }

}

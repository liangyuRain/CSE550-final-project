package paxos;

import lombok.extern.java.Log;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
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

    private ScheduledThreadPoolExecutor executor;

    private Level logLevel;

    public Node(Address address) throws IOException {
        this.address = address;
        this.logLevel = Level.ALL;

        FileHandler fh = new FileHandler(String.format("%s.log", this.getClass().getSimpleName()),
                LOG_FILE_SIZE_LIMIT, LOG_FILE_COUNT);
        fh.setFormatter(new SimpleFormatter());
        LOG.addHandler(fh);
    }

    // let child call init first
    void init() {
        this.executor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(50);
    }

    void listen() {
        try {
            ServerSocket serverSkt = new ServerSocket(Address.PORT);
            for (; ; ) {
                try {
                    Socket clientSocket = serverSkt.accept();
                    executor.execute(new ReceiveTask(clientSocket));
                } catch (IOException e) {
                    log(Level.SEVERE, String.format("Listen failed with %s", e.toString()));
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
            LOG.info(String.format("[Server %s] %s", address().hostname(), s));
        }
    }

    protected void log(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        pw.write(' ');
        pw.write(e.toString());
        pw.flush();
        log(Level.SEVERE, sw.toString());
        pw.close();
    }

    protected void send(Message message, Address to) {
        log(message.logLevel(), String.format("Send message %s to %s", message, to));
        executor.execute(new SendTask(message, to));
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
        executor.schedule(new TimeoutTask(timeout), timeout.timeoutLengthMillis(), TimeUnit.MILLISECONDS);
        log(timeout.logLevel(), String.format("Timeout %s set", timeout));
    }

    private class TimeoutTask extends TimerTask {

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
                } catch (NoSuchMethodException | IllegalAccessException e) {
                    log(e);
                    System.exit(1);
                } catch (InvocationTargetException e) {
                    log(e.getCause());
                    System.exit(1);
                }
            } catch (Exception e) {
                log(e);
                System.exit(1);
            }
        }

    }

    private class SendTask implements Runnable {

        public static final int CONNECTION_TIMEOUT = 3000;

        private final String str;
        private final byte[] pkg;
        private final Address to;

        public SendTask(Message message, Address to) {
            ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
            try {
                ObjectOutputStream objOutput = new ObjectOutputStream(byteOutput);
                objOutput.writeObject(new Package(Node.this.address, message));
                objOutput.flush();
                objOutput.close();
            } catch (IOException e) {
                log(e);
                System.exit(1);
            }
            this.pkg = byteOutput.toByteArray();
            this.str = message.toString();
            this.to = to;
        }

        @Override
        public void run() {
            try {
                Socket socket = new Socket();
                socket.connect(to.inetSocketAddress(), CONNECTION_TIMEOUT);
                OutputStream sktOutput = socket.getOutputStream();
                sktOutput.write(this.pkg);
                sktOutput.close();
                socket.close();
            } catch (IOException e) {
                log(Level.SEVERE, String.format("Send %s to %s failed with %s",
                        this.str, to.hostname(), e.toString()));
            } catch (Exception e) {
                log(e);
                System.exit(1);
            }
        }

    }

    private void handleMessage(Message message, Address sender) {
        Class<?> messageClass = message.getClass();
        try {
            Method method = this.getClass().getDeclaredMethod(
                    "handle" + messageClass.getSimpleName(), messageClass, Address.class);
            method.setAccessible(true);
            method.invoke(this, message, sender);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            log(e);
            System.exit(1);
        } catch (InvocationTargetException e) {
            log(e.getCause());
            System.exit(1);
        }
    }

    private class ReceiveTask implements Runnable {

        private final Socket clientSocket;

        public ReceiveTask(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try {
                InputStream sktinput = null;
                try {
                    sktinput = clientSocket.getInputStream();
                } catch (IOException e) {
                    log(Level.SEVERE, String.format("Receive failed with %s", e.toString()));
                    return;
                }
                ObjectInputStream objInput = null;
                Package pkg = null;
                try {
                    objInput = new ObjectInputStream(sktinput);
                    pkg = (Package) objInput.readObject();
                } catch (ClassNotFoundException | IOException e) {  // Incomplete package?
                    log(Level.SEVERE, String.format("Parse failed with %s", e.toString()));
                    return;
                }
                Message message = pkg.message();
                Address sender = pkg.sender();
                log(message.logLevel(), String.format("Got message %s from %s", message, sender));
                Node.this.handleMessage(message, sender);
                try {
                    objInput.close();
                    sktinput.close();
                } catch (IOException e) {
                    log(Level.SEVERE, String.format("Close failed with %s", e.toString()));
                    return;
                }
            } catch (Exception e) {
                log(e);
                System.exit(1);
            }
        }

    }

}

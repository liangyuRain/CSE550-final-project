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
import java.util.logging.Level;

@Log
public class Node {

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT %1$tL] %5$s %n");
    }

    private final Address address;

    private static final int TIMER_THREAD_POOL_SIZE = 8;
    private final Queue<Timer> timer_thread_pool;
    private ThreadPoolExecutor executor;

    private Level logLevel;

    public Node(Address address) {
        this.address = address;
        this.timer_thread_pool = new ConcurrentLinkedQueue<>();
        this.logLevel = Level.ALL;
    }

    // let child call init first
    void init() {
        for (int i = 0; i < TIMER_THREAD_POOL_SIZE; ++i) {
            timer_thread_pool.add(new Timer());
        }
        this.executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
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
            e.printStackTrace();
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

    protected void send(Message message, Address to) {
        log(Level.FINER, String.format("Send message %s to %s", message, to));
        executor.execute(new SendTask(message, to));
    }

    protected void broadcast(Message message, Collection<Address> to) {
        broadcast(message, to.toArray(new Address[0]));
    }

    protected void broadcast(Message message, Address[] to) {
        log(Level.FINER, String.format("Broadcast %s", message));
        for (Address addr : to) {
            send(message, addr);
        }
    }

    protected void set(Timeout timeout) {
        Timer timer = this.timer_thread_pool.poll();
        if (timer == null) {
            timer = new Timer();
        }
        timer.schedule(new TimeoutTask(timeout, timer), timeout.timeoutLengthMillis());
        log(Level.FINEST, String.format("Timeout %s set", timeout));
    }

    private class TimeoutTask extends TimerTask {

        private final Timeout timeout;
        private final Timer timer;

        public TimeoutTask(Timeout timeout, Timer timer) {
            this.timeout = timeout;
            this.timer = timer;
        }

        @Override
        public void run() {
            log(Level.FINEST, String.format("Timeout %s triggered", timeout));
            if (timer_thread_pool.size() < TIMER_THREAD_POOL_SIZE) {
                timer_thread_pool.add(timer);
            }
            Class<?> timeout_class = timeout.getClass();
            try {
                Method method = Node.this.getClass().getDeclaredMethod(
                        "on" + timeout_class.getSimpleName(), timeout_class);
                method.setAccessible(true);
                method.invoke(Node.this, timeout);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

    }

    private class SendTask implements Runnable {

        private final Message message;
        private final Address to;

        public SendTask(Message message, Address to) {
            this.message = message;
            this.to = to;
        }

        @Override
        public void run() {
            if (this.to.equals(Node.this.address)) {
                Node.this.handleMessage(this.message, Node.this.address);
            } else {
                try {
                    Socket socket = new Socket(to.inetAddress(), Address.PORT);
                    try {
                        OutputStream sktOutput = socket.getOutputStream();
                        ObjectOutputStream objOutput = new ObjectOutputStream(sktOutput);
                        objOutput.writeObject(new Package(Node.this.address, message));
                        objOutput.close();
                        sktOutput.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                    socket.close();
                } catch (IOException e) {
                    log(Level.SEVERE, String.format("Send to %s failed with %s", to.hostname(), e.toString()));
                }
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
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
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
                InputStream sktinput = clientSocket.getInputStream();
                try {
                    ObjectInputStream objInput = new ObjectInputStream(sktinput);
                    Package pkg = (Package) objInput.readObject();
                    Message message = pkg.message();
                    Address sender = pkg.sender();
                    log(Level.FINER, String.format("Got message %s from %s", message, sender));
                    Node.this.handleMessage(message, sender);
                    objInput.close();
                } catch (ClassNotFoundException | IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
                sktinput.close();
            } catch (IOException e) {
                log(Level.SEVERE, String.format("Receive failed with %s", e.toString()));
            }
        }

    }

}

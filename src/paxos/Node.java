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

@Log
public class Node {

    private final Address address;

    private static final int TIMER_THREAD_POOL_SIZE = 8;
    private final Queue<Timer> timer_thread_pool;
    private ThreadPoolExecutor executor;

    public Node(Address address) {
        this.address = address;
        this.timer_thread_pool = new ConcurrentLinkedQueue<>();
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
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public final Address address() {
        return address;
    }

    protected void send(Message message, Address to) {
        executor.execute(new SendTask(message, to));
        LOG.info(String.format("Send message %s to %s", message, to));
    }

    protected void broadcast(Message message, Collection<Address> to) {
        broadcast(message, to.toArray(new Address[0]));
    }

    protected void broadcast(Message message, Address[] to) {
        for (Address addr : to) {
            send(message, addr);
        }
        LOG.info(String.format("Broadcast %s", message));
    }

    protected void set(Timeout timeout) {
        Timer timer = this.timer_thread_pool.poll();
        if (timer == null) {
            timer = new Timer();
        }
        timer.schedule(new TimeoutTask(timeout, timer), timeout.timeoutLengthMillis());
        LOG.info(String.format("Timeout %s set", timeout));
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
            LOG.info(String.format("Timeout %s triggered", timeout));
            if (timer_thread_pool.size() < TIMER_THREAD_POOL_SIZE) {
                timer_thread_pool.add(timer);
            }
            Class<?> timeout_class = timeout.getClass();
            try {
                Method method = Node.this.getClass().getMethod(
                        "on" + timeout_class.getSimpleName(), timeout_class);
                method.invoke(Node.this, timeout);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
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
                    Socket socket = new Socket(to.getInetAddress(), Address.PORT);
                    OutputStream sktOutput = socket.getOutputStream();
                    ObjectOutputStream objOutput = new ObjectOutputStream(sktOutput);
                    objOutput.writeObject(new Package(Node.this.address, message));
                    objOutput.close();
                    sktOutput.close();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private void handleMessage(Message message, Address sender) {
        Class<?> messageClass = message.getClass();
        try {
            Method method = this.getClass().getMethod(
                    "handle" + messageClass.getSimpleName(), messageClass, Address.class);
            method.invoke(this, message, sender);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
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
                ObjectInputStream objInput = new ObjectInputStream(sktinput);
                Package pkg = (Package) objInput.readObject();
                objInput.close();
                sktinput.close();
                Message message = pkg.message();
                Address sender = pkg.sender();
                LOG.info(String.format("Got message %s from %s", message, sender));
                Node.this.handleMessage(message, sender);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

    }

}

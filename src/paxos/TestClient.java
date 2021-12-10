package paxos;

import application.Application;
import application.LockApplication;
import application.LockCommand;
import application.Result;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;

import java.lang.Thread;

public class TestClient {

    public static final int TEST_KEY_NUM = 10;
    public static LongAdder count = new LongAdder();

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.out.println("Usage: java -jar test_client.jar [local IPv4 address] [server ips config]");
            System.exit(1);
        }

        Address localAddr = Address.parseIPv4(args[0]);
        Address[] addrs = Address.getServerAddresses(args[1]);

        long signature = new Random().nextLong();

        PaxosClient client = new PaxosClient(localAddr, addrs);
        client.setLogLevel(Level.OFF);
        client.init();
        new Thread(client::listen).start();

        Application app = new LockApplication();
        Random r = new Random();

        new Thread(() -> {
            try {
                for (; ; ) {
                    long beginCount = count.sum();
                    long beginTime = System.nanoTime();
                    Thread.sleep(1000);
                    long endCount = count.sum();
                    long endTime = System.nanoTime();
                    double throughput = (endCount - beginCount) / ((endTime - beginTime) / 1.0e9);
                    client.log(Level.INFO, String.format("Test throughput: %.3f commands per second", throughput));
                    System.out.printf("[%s] Test throughput: %.3f commands per second%n",
                            client.address().hostname(), throughput);
                }
            } catch (Throwable e) {
                client.log(e);
                System.exit(1);
            }
        }).start();

        for (; ; ) {
            long locknum = signature * (1 + r.nextInt(TEST_KEY_NUM));
            LockCommand.Operation opt = r.nextBoolean() ? LockCommand.Operation.LOCK : LockCommand.Operation.UNLOCK;

            LockCommand cmd = new LockCommand(opt, locknum, signature);

            client.log(Level.FINE, String.format("Sending command: %s%n", cmd));

            long startTime = System.nanoTime();

            client.sendCommand(cmd);
            Result res = client.getResult();

            long endTime = System.nanoTime();
            long timeCost = endTime - startTime;
            Result res2 = app.execute(cmd);
            client.log(Level.FINE, String.format(
                    "Received result: %s, Expected result: %s, Time cost: %.3f ms, Command: %s",
                    res, res2, timeCost / 1.0e6, cmd));

            if (!res.equals(res2)) {
                System.out.printf(
                        "[%s] Result does not match, Received result: %s, Expected result: %s, Command: %s%n",
                        client.address().hostname(), res, res2, cmd);
                client.log(Level.SEVERE, String.format("Result does not match: " +
                        "Received result: %s, Expected result: %s, Command: %s", res, res2, cmd));
                System.exit(1);
            }

            count.increment();

//            Thread.sleep(1000);
        }
    }

}

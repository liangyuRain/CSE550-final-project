package paxos;

import application.Application;
import application.LockApplication;
import application.LockCommand;
import application.Result;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;
import java.util.logging.Level;

import java.lang.Thread;

public class TestClient {

    public static final int TEST_KEY_NUM = 10;

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 1) {
            System.out.println("Usage: java -jar test_client.jar [server ips config]");
            System.out.println("Missing [server ips config]");
            System.exit(1);
        }

        Address localAddr = Address.getLocalAddress();
        Address[] addrs = Address.getServerAddresses(args[0]);

        long signature = new Random().nextLong();

        PaxosClient client = new PaxosClient(localAddr, addrs);
        client.setLogLevel(Level.OFF);
        client.init();
        new Thread(client::listen).start();

        Application app = new LockApplication();
        Random r = new Random();

        Queue<Long> timestamps = new ArrayDeque<>();
        for (; ; ) {
            long locknum = signature * (1 + r.nextInt(TEST_KEY_NUM));
            LockCommand.Operation opt = r.nextBoolean() ? LockCommand.Operation.LOCK : LockCommand.Operation.UNLOCK;

            LockCommand cmd = new LockCommand(opt, locknum, signature);

            System.out.printf("Sending command: %s%n", cmd);

            long startTime = System.nanoTime();
            timestamps.add(startTime);

            client.sendCommand(cmd);
            Result res = client.getResult();

            long endTime = System.nanoTime();
            long timeCost = endTime - startTime;
            while (!timestamps.isEmpty() && timestamps.peek() < endTime - 1e9) {
                timestamps.remove();
            }
            double throughput = timestamps.isEmpty() ?
                    1 / (timeCost / 1.0e9) : timestamps.size() / ((endTime - timestamps.peek()) / 1.0e9);
            Result res2 = app.execute(cmd);
            System.out.printf("Received result: %s, Expected result: %s, Time cost: %.3f ms%n",
                    res, res2, timeCost / 1.0e6);
            client.log(Level.INFO, String.format(
                    "Received result: %s, Expected result: %s, Time cost: %.3f ms, " +
                            "Test throughput: %.3f per second, Command: %s",
                    res, res2, timeCost / 1.0e6, throughput, cmd));

            if (!res.equals(res2)) {
                System.out.println("Result does not match");
                client.log(Level.SEVERE, String.format("Result does not match: " +
                        "Received result: %s, Expected result: %s, Command: %s", res, res2, cmd));
                System.exit(1);
            }

            Thread.sleep(1000);
        }
    }

}

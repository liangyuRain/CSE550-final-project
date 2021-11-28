package paxos;

import application.Application;
import application.LockApplication;
import application.LockCommand;
import application.Result;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
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
        new Thread(new Runnable() {
            @Override
            public void run() {
                client.listen();
            }
        }).start();

        Application app = new LockApplication();
        Random r = new Random();

        for (; ; ) {
            long locknum = signature * (1 + r.nextInt(TEST_KEY_NUM));
            LockCommand.Operation opt = r.nextBoolean() ? LockCommand.Operation.LOCK : LockCommand.Operation.UNLOCK;

            LockCommand cmd = new LockCommand(opt, locknum, signature);

            System.out.printf("Sending command: %s%n", cmd);

            client.sendCommand(cmd);
            synchronized (client) {
                while (!client.hasResult()) {
                    client.wait(1000);
                }
            }
            Result res = client.getResult();
            Result res2 = app.execute(cmd);

            System.out.printf("Received result: %s, Expected result: %s%n", res, res2);
            client.log(Level.INFO, String.format(
                    "Received result: %s, Expected result: %s, Command: %s", res, res2, cmd));

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

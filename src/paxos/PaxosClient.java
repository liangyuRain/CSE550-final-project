package paxos;

import application.*;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.NotImplementedException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.logging.Level;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
    private final Address[] servers;

    private AMOCommand currentCommand = null;
    private AMOResult result = null;
    private int seqNum = 0;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosClient(Address address, Address[] servers) throws IOException {
        super(address);
        this.servers = servers;
    }

    @Override
    public synchronized void init() {
        super.init();
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command operation) {
        currentCommand = new AMOCommand(operation, ++seqNum, address());
        broadcast(new PaxosRequest(currentCommand), servers);
        set(new ClientTimeout(currentCommand));
    }

    @Override
    public synchronized boolean hasResult() {
        return result != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        while (result == null) {
            wait();
        }
        Result r = result.result();
        result = null;
        currentCommand = null;
        return r;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        if (currentCommand != null && result == null &&
                currentCommand.sequenceNum() == m.amoResult().sequenceNum()) {
            result = m.amoResult();
            notify();
        }
    }

    /* -------------------------------------------------------------------------
        Timeout Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimeout(ClientTimeout t) {
        if (t.command().equals(currentCommand) && result == null) {
            System.out.printf("Command %s timeout%n", currentCommand);

            broadcast(new PaxosRequest(currentCommand), servers);
            set(t);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 1) {
            System.out.println("Usage: java -jar paxos_client.jar [server ips config]");
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

        Scanner console = new Scanner(System.in);
        for (; ; ) {
            System.out.print("> ");

            String line = console.nextLine();
            StringTokenizer st = new StringTokenizer(line);

            LockCommand cmd = null;
            try {
                String opt = st.nextToken().toUpperCase();
                long locknum = Long.parseLong(st.nextToken());


                if (opt.equals("LOCK")) {
                    cmd = new LockCommand(LockCommand.Operation.LOCK, locknum, signature);
                } else if (opt.equals("UNLOCK")) {
                    cmd = new LockCommand(LockCommand.Operation.UNLOCK, locknum, signature);
                } else {
                    throw new IllegalArgumentException();
                }
            } catch (Exception e) {
                System.out.println("Illegal input, usage: [lock/unlock] [locknum]");
            }

            if (cmd != null) {
                client.sendCommand(cmd);
                synchronized (client) {
                    while (!client.hasResult()) {
                        client.wait(1000);
                    }
                }
                Result res = client.getResult();

                System.out.printf("%s%n", res.toString());
            }
        }
    }

}

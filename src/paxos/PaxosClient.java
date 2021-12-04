package paxos;

import application.*;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.logging.Level;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
    private final Address[] servers;

    private AMOCommand currentCommand = null;
    private long startTime = -1;
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
        startTime = System.nanoTime();
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
            log(Level.FINEST, String.format(
                    "Time cost %.3f ms for command: %s", (System.nanoTime() - startTime) / 1.0e6, currentCommand));
            result = m.amoResult();
            notify();
        }
    }

    /* -------------------------------------------------------------------------
        Timeout Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimeout(ClientTimeout t) {
        if (t.command().equals(currentCommand) && result == null) {
            System.out.printf("[%s] Command %s timeout%n", address().hostname(), currentCommand);

            broadcast(new PaxosRequest(currentCommand), servers);
            set(t);
        }
    }

    /* -------------------------------------------------------------------------
        Main Method
       -----------------------------------------------------------------------*/
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
        new Thread(client::listen).start();

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
                Result res = client.getResult();

                System.out.printf("Result: %s%nTime cost: %.3f ms%n",
                        res, (System.nanoTime() - client.startTime) / 1.0e6);
            }
        }
    }

}

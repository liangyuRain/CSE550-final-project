package paxos;

import application.*;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
    private final Address[] servers;

    private AMOCommand currentCommand = null;
    private long startTime = -1;
    private AMOResult result = null;
    private int seqNum = 0;

    @Override
    protected boolean PRINT_LOG_ESSENTIAL() {
        return false;
    }

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosClient(Address address, Address[] servers) throws IOException {
        super(address, (ThreadPoolExecutor) Executors.newCachedThreadPool(), 1);
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
            broadcast(new PaxosRequest(currentCommand), servers);
            set(t);
        }
    }

    /* -------------------------------------------------------------------------
        Main Method
       -----------------------------------------------------------------------*/

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.out.println("Usage: java -jar paxos_client.jar [local IPv4 address] [server ips config]");
            System.exit(1);
        }

        Address localAddr = Address.parseIPv4(args[0]);
        Address[] addrs = Address.getServerAddresses(args[1]);

        long signature = new Random().nextLong();

        PaxosClient client = new PaxosClient(localAddr, addrs);
        client.setLogLevel(Level.OFF);
        client.init();
        new Thread(client::listen).start();

        Scanner console = new Scanner(System.in);
        for (; ; ) {
            System.out.print("> ");

            if (!console.hasNextLine()) {
                System.exit(0);
            }

            String line = console.nextLine();
            StringTokenizer st = new StringTokenizer(line);

            LockCommand cmd = null;
            try {
                String opt = st.nextToken().toUpperCase();

                long[] locknums = new long[st.countTokens()];
                for (int i = 0; i < locknums.length; ++i) {
                    locknums[i] = Long.parseLong(st.nextToken());
                }

                if (opt.equals("LOCK")) {
                    cmd = new LockCommand(LockCommand.Operation.LOCK, locknums, signature);
                } else if (opt.equals("UNLOCK")) {
                    cmd = new LockCommand(LockCommand.Operation.UNLOCK, locknums, signature);
                } else if (opt.equals("QUERY")) {
                    cmd = new LockCommand(LockCommand.Operation.QUERY, locknums, signature);
                } else {
                    throw new IllegalArgumentException();
                }
            } catch (Exception e) {
                System.out.println("Illegal input, usage: [lock/unlock/query] [locknum_1] [locknum_2] ...");
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

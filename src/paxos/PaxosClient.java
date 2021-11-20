package paxos;

import application.AMOCommand;
import application.AMOResult;
import application.Command;
import application.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;


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
    public PaxosClient(Address address, Address[] servers) {
        super(address);
        this.servers = servers;
    }

    @Override
    public synchronized void init() {
        // No need to initialize
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
            broadcast(new PaxosRequest(currentCommand), servers);
            set(t);
        }
    }
}

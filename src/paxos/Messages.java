package paxos;


// Your code here...

import application.AMOCommand;
import lombok.Data;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.*;

import java.util.LinkedHashSet;
import java.util.logging.Level;

@Data
class Ping implements Message {

    private final int nextToExecute;

    @Override
    public Level logLevel() {
        return Level.FINEST;
    }

}

@Data
class PrepareRequest implements Message {

    @NonNull
    private final Pair<Integer, Address> proposalNum;

    @Override
    public Level logLevel() {
        return Level.FINER;
    }

}

@Data
class PrepareReply implements Message { // success iff proposalNum >= maxProposalNum

    @NonNull
    private final Pair<Integer, Address> proposalNum;
    private final PaxosServer.Slots executed;
    private final LinkedHashSet<AMOCommand> uncertain;
    @NonNull
    private final Triple<Integer, Address, Integer> maxAcceptNum;
    @NonNull
    private final Pair<Integer, Address> maxProposalNum;

    @Override
    public Level logLevel() {
        return Level.FINER;
    }

}

@Data
class AcceptRequest implements Message {

    @NonNull
    private final Triple<Integer, Address, Integer> acceptNum;
    @NonNull
    private final PaxosServer.Slots executed;
    @NonNull
    private final LinkedHashSet<AMOCommand> uncertain;
    private final int nextToExecute;

    @Override
    public Level logLevel() {
        return Level.FINER;
    }

}

@Data
class AcceptReply implements Message { // success iff acceptNum >= maxProposalNum

    @NonNull
    private final Triple<Integer, Address, Integer> acceptNum;
    @NonNull
    private final Pair<Integer, Address> maxProposalNum;
    private final int nextToExecute;

    @Override
    public Level logLevel() {
        return Level.FINER;
    }

}

@Data
class TestAlive implements Message {

    private final long timestamp; // System.currentTimeMillis()

    @Override
    public Level logLevel() {
        return Level.FINEST;
    }

}

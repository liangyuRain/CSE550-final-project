package paxos;

import application.AMOCommand;
import lombok.*;
import org.apache.commons.lang3.tuple.*;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.logging.Level;

@Value
class Ping implements Message {

    @NonNull
    List<ImmutablePair<Address, Integer>> serverExecuted;

    @EqualsAndHashCode.Exclude
    boolean copied;

    @Override
    public Level logLevel() {
        return Level.FINEST;
    }

    @Override
    public Ping immutableCopy() {
        if (copied) return this;
        return new Ping(new ArrayList<>(serverExecuted), true);
    }

}

@Value
class PrepareRequest implements Message {

    @NonNull
    ImmutablePair<Integer, Address> proposalNum;
    int nextToExecute;

    @Override
    public Level logLevel() {
        return Level.FINER;
    }

    @Override
    public PrepareRequest immutableCopy() {
        return this;
    }

}

@Value
class PrepareReply implements Message { // success iff proposalNum >= maxProposalNum

    @NonNull
    ImmutablePair<Integer, Address> proposalNum;
    PaxosServer.Slots executed;
    LinkedHashSet<AMOCommand> uncertain;
    @NonNull
    ImmutableTriple<Integer, Address, Integer> maxAcceptNum;
    @NonNull
    ImmutablePair<Integer, Address> maxProposalNum;

    @EqualsAndHashCode.Exclude
    boolean copied;

    @Override
    public Level logLevel() {
        return Level.FINER;
    }

    @Override
    public PrepareReply immutableCopy() {
        if (copied) return this;
        return new PrepareReply(
                proposalNum,
                executed == null ? null : executed.immutableCopy(),
                uncertain == null ? null : new LinkedHashSet<>(uncertain),
                maxAcceptNum,
                maxProposalNum,
                true
        );
    }

}

@Value
class AcceptRequest implements Message {

    @NonNull
    ImmutableTriple<Integer, Address, Integer> acceptNum;
    @NonNull
    PaxosServer.Slots executed;
    @NonNull
    LinkedHashSet<AMOCommand> uncertain;

    @EqualsAndHashCode.Exclude
    boolean copied;

    @Override
    public Level logLevel() {
        return Level.FINER;
    }

    @Override
    public AcceptRequest immutableCopy() {
        if (copied) return this;
        return new AcceptRequest(
                acceptNum,
                executed.immutableCopy(),
                new LinkedHashSet<>(uncertain),
                true
        );
    }

}

@Value
class AcceptReply implements Message { // success iff acceptNum >= maxProposalNum

    @NonNull
    ImmutableTriple<Integer, Address, Integer> acceptNum;
    @NonNull
    ImmutablePair<Integer, Address> maxProposalNum;
    int nextToExecute;

    @Override
    public Level logLevel() {
        return Level.FINER;
    }

    @Override
    public AcceptReply immutableCopy() {
        return this;
    }

}

@Value
class TestAlive implements Message {

    long timestamp; // System.nanoTime()

    @Override
    public Level logLevel() {
        return Level.FINEST;
    }

    @Override
    public TestAlive immutableCopy() {
        return this;
    }

}

@Value
class Recover implements Message {

    @NonNull
    PaxosServer.Slots executed;
    int nextToExecute;

    @EqualsAndHashCode.Exclude
    boolean copied;

    @Override
    public Level logLevel() {
        return Level.FINER;
    }

    @Override
    public Recover immutableCopy() {
        if (copied) return this;
        return new Recover(executed.immutableCopy(), nextToExecute, true);
    }

}

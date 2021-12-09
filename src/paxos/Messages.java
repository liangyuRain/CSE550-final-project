package paxos;

import application.AMOCommand;
import lombok.Data;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.*;

import java.util.LinkedHashSet;
import java.util.logging.Level;
import java.util.stream.Collectors;

@Data
class Ping implements Message {

    private final int nextToExecute;

    @Override
    public Level logLevel() {
        return Level.FINEST;
    }

    @Override
    public Ping immutableCopy() {
        return this;
    }

}

@Data
class PrepareRequest implements Message {

    @NonNull
    private final ImmutablePair<Integer, Address> proposalNum;
    private final int nextToExecute;

    @Override
    public Level logLevel() {
        return Level.FINER;
    }

    @Override
    public PrepareRequest immutableCopy() {
        return this;
    }

}

@Data
class PrepareReply implements Message { // success iff proposalNum >= maxProposalNum

    @NonNull
    private final ImmutablePair<Integer, Address> proposalNum;
    private final PaxosServer.Slots executed;
    private final LinkedHashSet<AMOCommand> uncertain;
    @NonNull
    private final ImmutableTriple<Integer, Address, Integer> maxAcceptNum;
    @NonNull
    private final ImmutablePair<Integer, Address> maxProposalNum;

    @Override
    public Level logLevel() {
        return Level.FINER;
    }

    @Override
    public PrepareReply immutableCopy() {
        return new PrepareReply(
                proposalNum,
                executed == null ? null : executed.immutableCopy(),
                uncertain == null ? null : uncertain.stream()
                        .map(AMOCommand::immutableCopy)
                        .collect(Collectors.toCollection(LinkedHashSet::new)),
                maxAcceptNum,
                maxProposalNum
        );
    }

}

@Data
class AcceptRequest implements Message {

    @NonNull
    private final ImmutableTriple<Integer, Address, Integer> acceptNum;
    @NonNull
    private final PaxosServer.Slots executed;
    @NonNull
    private final LinkedHashSet<AMOCommand> uncertain;

    @Override
    public Level logLevel() {
        return Level.FINER;
    }

    @Override
    public AcceptRequest immutableCopy() {
        return new AcceptRequest(
                acceptNum,
                executed.immutableCopy(),
                uncertain.stream()
                        .map(AMOCommand::immutableCopy)
                        .collect(Collectors.toCollection(LinkedHashSet::new))
        );
    }

}

@Data
class AcceptReply implements Message { // success iff acceptNum >= maxProposalNum

    @NonNull
    private final ImmutableTriple<Integer, Address, Integer> acceptNum;
    @NonNull
    private final ImmutablePair<Integer, Address> maxProposalNum;
    private final int nextToExecute;

    @Override
    public Level logLevel() {
        return Level.FINER;
    }

    @Override
    public AcceptReply immutableCopy() {
        return this;
    }

}

@Data
class TestAlive implements Message {

    private final long timestamp; // System.nanoTime()

    @Override
    public Level logLevel() {
        return Level.FINEST;
    }

    @Override
    public TestAlive immutableCopy() {
        return this;
    }

}

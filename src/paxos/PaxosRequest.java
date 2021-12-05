package paxos;

import application.AMOCommand;
import lombok.Data;

import java.util.logging.Level;

@Data
public final class PaxosRequest implements Message {

    private final AMOCommand command;

    @Override
    public Level logLevel() {
        return Level.FINER;
    }

    @Override
    public PaxosRequest immutableCopy() {
        return new PaxosRequest(command.immutableCopy());
    }

}

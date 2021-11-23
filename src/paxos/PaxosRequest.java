package paxos;

import application.AMOCommand;
import lombok.Data;

import java.util.logging.Level;

@Data
public final class PaxosRequest implements Message {
	// Your code here...
    private final AMOCommand command;

    @Override
    public Level logLevel() {
        return Level.FINER;
    }

}

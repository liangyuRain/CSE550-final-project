package paxos;

import application.AMOCommand;
import lombok.Data;

@Data
public final class PaxosRequest implements Message {
	// Your code here...
    private final AMOCommand command;

}

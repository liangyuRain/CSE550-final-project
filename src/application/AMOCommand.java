package application;

import paxos.Address;
import lombok.Data;

@Data
public final class AMOCommand implements Command {
	// Your code here...
    private final Command command;
    private final int sequenceNum;
    private final Address clientAddr;
}

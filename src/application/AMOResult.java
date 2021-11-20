package application;

import paxos.Address;
import lombok.Data;

@Data
public final class AMOResult implements Result {
	// Your code here...
    private final Result result;
    private final int sequenceNum;
    private final Address clientAddr;
}

package paxos;

import application.AMOResult;
import lombok.Data;

@Data
public final class PaxosReply implements Message {
	// Your code here...
    private final Address leader;
    private final AMOResult amoResult;

}

package application;

import lombok.Value;
import paxos.Address;

@Value
public class AMOCommand implements Command {

    Command command;
    int sequenceNum;
    Address clientAddr;

    @Override
    public AMOCommand immutableCopy() {
        return this;
    }

}

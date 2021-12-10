package application;

import lombok.Value;
import paxos.Address;

@Value
public class AMOResult implements Result {

    Result result;
    int sequenceNum;
    Address clientAddr;

    @Override
    public AMOResult immutableCopy() {
        return this;
    }

}

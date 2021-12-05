package application;

import paxos.Address;
import lombok.Data;

@Data
public final class AMOResult implements Result {

    private final Result result;
    private final int sequenceNum;
    private final Address clientAddr;

    @Override
    public AMOResult immutableCopy() {
        return new AMOResult(result.immutableCopy(), sequenceNum, clientAddr.immutableCopy());
    }

}

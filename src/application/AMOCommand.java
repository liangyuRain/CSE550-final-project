package application;

import paxos.Address;
import lombok.Data;

@Data
public final class AMOCommand implements Command {

    private final Command command;
    private final int sequenceNum;
    private final Address clientAddr;

    @Override
    public AMOCommand immutableCopy() {
        return new AMOCommand(command.immutableCopy(), sequenceNum, clientAddr.immutableCopy());
    }

}

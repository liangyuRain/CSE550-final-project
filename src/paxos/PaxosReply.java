package paxos;

import application.AMOResult;
import lombok.Data;

import java.util.logging.Level;

@Data
public final class PaxosReply implements Message {

    private final Address leader;
    private final AMOResult amoResult;

    @Override
    public Level logLevel() {
        return Level.FINER;
    }

}

package paxos;

import java.io.Serializable;

public interface Timeout extends Serializable, Logged {
    int timeoutLengthMillis();
}

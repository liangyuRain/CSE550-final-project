package application;

import paxos.Copyable;

import java.io.Serializable;

// Any class implementing Command must be immutable
public interface Command extends Serializable, Copyable {

    default boolean readOnly() {
        return false;
    }

    @Override
    Command immutableCopy();

}

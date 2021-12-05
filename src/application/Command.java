package application;

import paxos.Copyable;

import java.io.Serializable;

public interface Command extends Serializable, Copyable {

    default boolean readOnly() {
        return false;
    }

    @Override
    Command immutableCopy();

}

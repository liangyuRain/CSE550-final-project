package application;

import paxos.Copyable;

import java.io.Serializable;

// Any class implementing Result must be immutable
public interface Result extends Serializable, Copyable {
    @Override
    Result immutableCopy();
}

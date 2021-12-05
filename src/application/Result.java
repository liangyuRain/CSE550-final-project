package application;

import paxos.Copyable;

import java.io.Serializable;

public interface Result extends Serializable, Copyable {
    @Override
    Result immutableCopy();
}

package application;

import lombok.Value;

import java.io.Serializable;

@Value
public class LockCommand implements Serializable, Command {

    public enum Operation {LOCK, UNLOCK, QUERY}

    Operation operation;
    long[] locknums;
    long signature;

    @Override
    public Command immutableCopy() {
        return this;
    }

}

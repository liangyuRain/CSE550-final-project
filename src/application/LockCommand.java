package application;

import lombok.Value;

import java.io.Serializable;

@Value
public class LockCommand implements Serializable, Command {

    public enum Operation {LOCK, UNLOCK}

    Operation operation;
    long locknum;
    long signature;

    @Override
    public Command immutableCopy() {
        return this;
    }

}

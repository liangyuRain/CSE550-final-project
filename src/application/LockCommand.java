package application;

import lombok.Data;

import java.io.Serializable;

@Data
public class LockCommand implements Serializable, Command {

    public enum Operation {LOCK, UNLOCK}

    private final Operation operation;
    private final long locknum;
    private final long signature;

}

package application;

import lombok.Data;

import java.io.Serializable;

@Data
public class LockCommand implements Serializable, Command {

    enum Operation {LOCK, UNLOCK}

    private Operation operation;
    private long locknum;
    private long signature;

}

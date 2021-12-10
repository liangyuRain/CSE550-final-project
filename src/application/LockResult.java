package application;

import lombok.Value;

import java.io.Serializable;

@Value
public class LockResult implements Serializable, Result {

    boolean res;
    long count;

    @Override
    public LockResult immutableCopy() {
        return this;
    }

}

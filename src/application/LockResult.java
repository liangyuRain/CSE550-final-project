package application;

import lombok.Value;

import java.io.Serializable;
import java.util.Set;

@Value
public class LockResult implements Serializable, Result {

    boolean success;
    Set<Long> locked;

    @Override
    public LockResult immutableCopy() {
        return this;
    }

}

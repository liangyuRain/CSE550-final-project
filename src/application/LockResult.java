package application;

import lombok.Data;
import lombok.Getter;

import java.io.Serializable;

@Data
public class LockResult implements Serializable, Result {

    @Getter
    private final boolean res;
    private final long count;

    @Override
    public LockResult immutableCopy() {
        return this;
    }

}

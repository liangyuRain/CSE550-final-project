package application;

import lombok.Data;
import lombok.Getter;

import java.io.Serializable;

@Data
public class LockResult implements Serializable, Result {

    @Getter
    private final boolean res;

    private LockResult(boolean res) {
        this.res = res;
    }

    public static final LockResult success = new LockResult(true);
    public static final LockResult failure = new LockResult(false);

}

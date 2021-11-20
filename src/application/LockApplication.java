package application;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockApplication implements Serializable, Application {

    private Map<Long, Long> locks;

    public LockApplication() {
        this.locks = new ConcurrentHashMap<>();
    }

    @Override
    public Result execute(Command command) {
        if (!(command instanceof LockCommand)) {
            return null;
        }
        LockCommand lockCommand = (LockCommand) command;

        long locknum = lockCommand.locknum();
        Long signature = lockCommand.signature();
        switch (lockCommand.operation()) {
            case LOCK:
                if (!locks.containsKey(locknum)) {
                    locks.put(locknum, signature);
                    return LockResult.success;
                } else {
                    return LockResult.failure;
                }
            case UNLOCK:
                if (signature.equals(locks.get(locknum))) {
                    locks.remove(locknum);
                    return LockResult.success;
                } else {
                    return LockResult.failure;
                }
            default:
                return null;
        }
    }

}

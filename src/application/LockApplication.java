package application;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class LockApplication implements Serializable, Application {

    private final Map<Long, Long> locks;
    private final Map<Long, Long> operationCounter;

    public LockApplication() {
        this.locks = new HashMap<>();
        this.operationCounter = new HashMap<>();
    }

    @Override
    public synchronized Result execute(Command command) {
        if (!(command instanceof LockCommand)) {
            return null;
        }
        LockCommand lockCommand = (LockCommand) command;

        long locknum = lockCommand.locknum();
        Long signature = lockCommand.signature();
        long count = operationCounter.getOrDefault(locknum, 0L) + 1;
        operationCounter.put(locknum, count);
        switch (lockCommand.operation()) {
            case LOCK:
                if (!locks.containsKey(locknum)) {
                    locks.put(locknum, signature);
                    return new LockResult(true, count);
                } else {
                    return new LockResult(false, count);
                }
            case UNLOCK:
                if (signature.equals(locks.get(locknum))) {
                    locks.remove(locknum);
                    return new LockResult(true, count);
                } else {
                    return new LockResult(false, count);
                }
            default:
                return null;
        }
    }

}

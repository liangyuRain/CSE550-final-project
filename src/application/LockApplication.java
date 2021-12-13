package application;

import java.io.Serializable;
import java.util.*;

public class LockApplication implements Serializable, Application {

    private final Map<Long, Long> lockToSig;
    private final Map<Long, Set<Long>> sigToLock;

    public LockApplication() {
        this.lockToSig = new HashMap<>();
        this.sigToLock = new HashMap<>();
    }

    @Override
    public synchronized Result execute(Command command) {
        if (!(command instanceof LockCommand)) {
            return null;
        }
        LockCommand lockCommand = (LockCommand) command;
        long[] locknums = lockCommand.locknums();
        long signature = lockCommand.signature();
        Set<Long> locks = sigToLock.computeIfAbsent(signature, k -> new HashSet<>());

        switch (lockCommand.operation()) {
            case LOCK:
                if (locks.isEmpty()) {
                    if (Arrays.stream(locknums).noneMatch(lockToSig::containsKey)) {
                        for (long locknum : locknums) {
                            if (lockToSig.put(locknum, signature) == null) {
                                locks.add(locknum);
                            }
                        }
                        return new LockResult(true, locks);
                    }
                } else {
                    if (Arrays.stream(locknums).allMatch(locks::contains)) {
                        return new LockResult(true, locks);
                    }
                }
                return new LockResult(false, locks);
            case UNLOCK:
                if (Arrays.stream(locknums).allMatch(n -> {
                    Long lockSig = lockToSig.get(n);
                    return lockSig == null || lockSig.equals(signature);
                })) {
                    for (long locknum : locknums) {
                        if (lockToSig.remove(locknum) != null) {
                            locks.remove(locknum);
                        }
                    }
                    return new LockResult(true, locks);
                } else {
                    return new LockResult(false, locks);
                }
            case QUERY:
                return new LockResult(true, locks);
            default:
                throw new IllegalArgumentException("Unknown operation");
        }
    }

}

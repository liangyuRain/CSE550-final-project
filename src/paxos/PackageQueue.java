package paxos;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PackageQueue {

    private final HashSet<Package> pkgSet;
    private final ArrayDeque<Package> queue;
    private final int capacity;

    private final ReentrantReadWriteLock lock;
    private final ReentrantReadWriteLock.WriteLock writeLock;
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final Condition notEmpty;

    public PackageQueue(int capacity) {
        this.pkgSet = new HashSet<>();
        this.queue = new ArrayDeque<>(capacity);
        this.capacity = capacity;

        this.lock = new ReentrantReadWriteLock();
        this.writeLock = lock.writeLock();
        this.readLock = lock.readLock();
        this.notEmpty = writeLock.newCondition();
    }

    public Package add(Package pkg) {
        writeLock.lock();
        Package ignored = null;
        if (!pkgSet.contains(pkg)) {
            assert(queue.size() <= capacity);
            if (queue.size() == capacity) {
                ignored = queue.removeFirst();
                pkgSet.remove(ignored);
            }
            queue.addLast(pkg);
            pkgSet.add(pkg);
            notEmpty.signal();
        }
        writeLock.unlock();
        return ignored;
    }

    public Package poll(long timeout) throws InterruptedException {
        long remainingTime = TimeUnit.MILLISECONDS.toNanos(timeout);
        writeLock.lock();
        Package pkg;
        while ((pkg = queue.pollFirst()) == null) {
            if (remainingTime < 0) {
                writeLock.unlock();
                return null;
            }
            remainingTime = notEmpty.awaitNanos(remainingTime);
        }
        pkgSet.remove(pkg);
        writeLock.unlock();
        return pkg;
    }

    public int remainingCapacity() {
        readLock.lock();
        int res = capacity - queue.size();
        readLock.unlock();
        return res;
    }

    public int size() {
        readLock.lock();
        int res = queue.size();
        readLock.unlock();
        return res;
    }

}

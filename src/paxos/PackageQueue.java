package paxos;

import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PackageQueue {

    private final HashMap<Package, Long> pkgMap;
    private final ArrayDeque<Package> queue;
    private final int capacity;

    private final ReentrantReadWriteLock lock;
    private final ReentrantReadWriteLock.WriteLock writeLock;
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final Condition notEmpty;

    private volatile int count;
    private volatile long totalDelay;

    public PackageQueue(int capacity) {
        this.pkgMap = new HashMap<>();
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
        if (!pkgMap.containsKey(pkg)) {
            assert(queue.size() <= capacity);
            if (queue.size() == capacity) {
                ignored = queue.removeFirst();
                pkgMap.remove(ignored);
            }
            queue.addLast(pkg);
            pkgMap.put(pkg, System.nanoTime());
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
        Long timestamp = pkgMap.remove(pkg);
        if (timestamp != null) {
            totalDelay += System.nanoTime() - timestamp;
            ++count;
        }
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

    public Pair<Integer, Long> stat() {
        readLock.lock();
        Pair<Integer, Long> res = Pair.of(count, totalDelay);
        readLock.unlock();
        return res;
    }

}

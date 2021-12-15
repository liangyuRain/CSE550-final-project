package paxos;

import lombok.ToString;
import lombok.extern.java.Log;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@Log
@ToString
public class ArrayBlockingSetQueue<E> extends ArrayBlockingQueue<E> {

    public enum FullQueuePolicy {DISCARD_LATEST, DISCARD_OLDEST}

    private final FullQueuePolicy policy;
    private final HashMap<E, Long> map;
    private final ReentrantLock lock;

    private int count;
    private long totalDelay;

    public ArrayBlockingSetQueue(int capacity) throws NoSuchFieldException, IllegalAccessException {
        this(capacity, FullQueuePolicy.DISCARD_LATEST);
    }

    public ArrayBlockingSetQueue(int capacity, FullQueuePolicy policy)
            throws NoSuchFieldException, IllegalAccessException {
        super(capacity, true);
        this.policy = policy;
        this.map = new HashMap<>();

        Field privateLock = this.getClass().getSuperclass().getDeclaredField("lock");
        privateLock.setAccessible(true);
        this.lock = (ReentrantLock) privateLock.get(this);
    }

    public E addWithDiscard(E e) {
        lock.lock();
        try {
            E ignored = null;
            if (!map.containsKey(e)) {
                boolean res = super.offer(e);
                if (!res) {
                    if (policy == FullQueuePolicy.DISCARD_OLDEST) {
                        ignored = super.poll();
                        if (ignored == null) throw new NoSuchElementException();
                        map.remove(ignored);
                        super.add(e);
                        map.put(e, System.nanoTime());
                    } else {
                        ignored = e;
                    }
                } else {
                    map.put(e, System.nanoTime());
                }
            }
            return ignored;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean add(E e) {
        lock.lock();
        try {
            if (!map.containsKey(e)) {
                super.add(e);
                map.put(e, System.nanoTime());
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E remove() {
        lock.lock();
        try {
            E res = super.poll();
            if (res == null) throw new NoSuchElementException();
            count += 1;
            totalDelay += System.nanoTime() - map.remove(res);
            return res;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            super.clear();
            map.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean contains(Object o) {
        if (o == null) return false;
        lock.lock();
        try {
            return map.containsKey(o);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        lock.lock();
        try {
            ArrayList<E> tmp = new ArrayList<>();
            int res = super.drainTo(tmp);
            c.addAll(tmp);
            count += tmp.size();
            long delay = tmp.stream().mapToLong(map::remove).sum();
            totalDelay += System.nanoTime() * tmp.size() - delay;
            return res;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        lock.lock();
        try {
            ArrayList<E> tmp = new ArrayList<>();
            int res = super.drainTo(tmp, maxElements);
            c.addAll(tmp);
            count += tmp.size();
            long delay = tmp.stream().mapToLong(map::remove).sum();
            totalDelay += System.nanoTime() * tmp.size() - delay;
            return res;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Iterator<E> iterator() {
        return new Itr();
    }

    @Override
    public boolean offer(E e) {
        lock.lock();
        try {
            boolean res = true;
            if (!map.containsKey(e)) {
                res = super.offer(e);
                if (res) map.put(e, System.nanoTime());
            }
            return res;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        lock.lock();
        try {
            boolean res = true;
            if (!map.containsKey(e)) {
                res = super.offer(e, timeout, unit);
                if (res) map.put(e, System.nanoTime());
            }
            return res;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E poll() {
        lock.lock();
        try {
            E res = super.poll();
            if (res != null) {
                count += 1;
                totalDelay += System.nanoTime() - map.remove(res);
            }
            return res;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        lock.lock();
        try {
            E res = super.poll(timeout, unit);
            if (res != null) {
                count += 1;
                totalDelay += System.nanoTime() - map.remove(res);
            }
            return res;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(E e) throws InterruptedException {
        lock.lock();
        try {
            if (!map.containsKey(e)) {
                super.put(e);
                map.put(e, System.nanoTime());
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean remove(Object o) {
        lock.lock();
        try {
            boolean res = false;
            if (map.remove(o) != null) {
                res = super.remove(o);
            }
            return res;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Spliterator<E> spliterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public E take() throws InterruptedException {
        lock.lock();
        try {
            E res = super.take();
            count += 1;
            totalDelay += System.nanoTime() - map.remove(res);
            return res;
        } finally {
            lock.unlock();
        }
    }

    public Pair<Integer, Long> stat() {
        lock.lock();
        try {
            return Pair.of(count, totalDelay);
        } finally {
            lock.unlock();
        }
    }

    private class Itr implements Iterator<E> {

        private final Iterator<E> parentIterator;

        Itr() {
            this.parentIterator = ArrayBlockingSetQueue.super.iterator();
        }

        @Override
        public boolean hasNext() {
            return parentIterator.hasNext();
        }

        @Override
        public E next() {
            return parentIterator.next();
        }

        @Override
        public void remove() {
        }

    }

}

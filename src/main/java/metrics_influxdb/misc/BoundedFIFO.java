package metrics_influxdb.misc;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BoundedFIFO<T> implements Queue<T> {
    private final LinkedBlockingQueue<T> delegate;
    
    public BoundedFIFO(int capacity) {
        this.delegate = new LinkedBlockingQueue<>(capacity);
    } 

    public int hashCode() {
        return delegate.hashCode();
    }

    public boolean add(T e) {
        while (!delegate.add(e)) {
            delegate.poll();
        }
        return true;
    }

    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    public T remove() {
        return delegate.remove();
    }

    public boolean equals(Object obj) {
        return delegate.equals(obj);
    }

    public T element() {
        return delegate.element();
    }

    public boolean addAll(Collection<? extends T> c) {
        return delegate.addAll(c);
    }

    public int size() {
        return delegate.size();
    }

    public int remainingCapacity() {
        return delegate.remainingCapacity();
    }

    public void put(T e) throws InterruptedException {
        delegate.put(e);
    }

    public boolean containsAll(Collection<?> c) {
        return delegate.containsAll(c);
    }

    public boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException {
        return offer(e);
    }

    public boolean removeAll(Collection<?> c) {
        return delegate.removeAll(c);
    }

    public boolean offer(T e) {
        while (!delegate.offer(e)) {
            delegate.poll();
        }
        return true;
    }

    public boolean retainAll(Collection<?> c) {
        return delegate.retainAll(c);
    }

    public T take() throws InterruptedException {
        return delegate.take();
    }

    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.poll(timeout, unit);
    }

    public T poll() {
        return delegate.poll();
    }

    public T peek() {
        return delegate.peek();
    }

    public boolean remove(Object o) {
        return delegate.remove(o);
    }

    public boolean contains(Object o) {
        return delegate.contains(o);
    }

    public Object[] toArray() {
        return delegate.toArray();
    }

    public <E> E[] toArray(E[] a) {
        return delegate.toArray(a);
    }

    public String toString() {
        return delegate.toString();
    }

    public void clear() {
        delegate.clear();
    }

    public int drainTo(Collection<? super T> c) {
        return delegate.drainTo(c);
    }

    public int drainTo(Collection<? super T> c, int maxElements) {
        return delegate.drainTo(c, maxElements);
    }

    public Iterator<T> iterator() {
        return delegate.iterator();
    }
}

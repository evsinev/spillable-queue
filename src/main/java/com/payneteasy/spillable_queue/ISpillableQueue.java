package com.payneteasy.spillable_queue;

import java.io.Closeable;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;

public interface ISpillableQueue<E extends Serializable> extends Closeable {

    /**
     * Adds an element. If the write buffer is full the oldest batch is spilled
     * to disk to make room. This call never blocks waiting for the reader.
     */
    void offer(E element);

    /**
     * Retrieves and removes the head element, or {@code null} if empty.
     * Non-blocking.
     */
    E poll();

    /**
     * Retrieves and removes the head element, blocking until one is available
     * or the queue is closed.
     *
     * @return the element, or {@code null} if the queue was closed while waiting
     */
    E take() throws InterruptedException;

    /**
     * Removes up to {@code maxElements} elements and adds them to a list.
     * Non-blocking: returns immediately, possibly with an empty list.
     *
     * @param maxElements maximum number of elements to drain
     * @return list of drained elements, never {@code null}
     */
    List<E> drainTo(int maxElements);

    /**
     * Removes up to {@code maxElements} elements and adds them to a list.
     * Blocks until at least one element is available or {@code timeout} elapses.
     * Returns an empty list if the queue is still empty after the timeout.
     *
     * @param maxElements maximum number of elements to drain
     * @param timeout     how long to wait for the first element
     * @return list of drained elements, never {@code null}
     */
    List<E> drainTo(int maxElements, Duration timeout) throws InterruptedException;

    /** Total number of elements across memory + disk. */
    long size();

    @Override
    void close();
}

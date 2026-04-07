package com.payneteasy.spillable_queue;

import java.io.Closeable;
import java.io.Serializable;

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

    /** Total number of elements across memory + disk. */
    long size();

    @Override
    void close();
}

package com.payneteasy.spillable_queue.impl;

import com.payneteasy.spillable_queue.ISpillableQueue;
import com.payneteasy.spillable_queue.ISpillableQueueSerializer;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A thread-safe bounded queue that keeps elements in memory up to a configurable
 * threshold, then spills overflow to disk. Designed for many-writers / single-reader.
 *
 * <h3>Design</h3>
 * Two internal buffers maintain strict FIFO ordering even when elements are
 * spilled to disk:
 * <pre>
 *   Writers ──► [ writeBuffer (bounded) ]
 *                     │ overflow
 *                     ▼
 *              [ disk spill files (FIFO) ]
 *                     │ refill
 *                     ▼
 *              [ readBuffer ] ──► Reader
 * </pre>
 *
 * <ul>
 *   <li><b>writeBuffer</b> — writers append here. When it reaches
 *       {@code memoryCapacity}, a batch is serialized to a spill file.</li>
 *   <li><b>readBuffer</b> — the reader drains this first. When it's empty,
 *       the oldest spill file is loaded into it. If there are no spill files,
 *       the writeBuffer is swapped in as the new readBuffer.</li>
 *   <li>This two-buffer scheme guarantees that disk-persisted (older) elements
 *       are always consumed before in-memory (newer) ones.</li>
 * </ul>
 *
 * @param <E> element type — must be {@link Serializable}
 */
public class SpillableQueueImpl<E extends Serializable> implements ISpillableQueue<E> {

    /* ──────────────────────── metrics ──────────────────────── */

    private static final Counter OFFERS_TOTAL = Counter.build()
            .name("spillable_queue_offered_total")
            .help("Total number of elements offered to the queue")
            .labelNames("queue_name")
            .register();

    private static final Counter POLLS_TOTAL = Counter.build()
            .name("spillable_queue_polled_total")
            .help("Total number of elements polled from the queue")
            .labelNames("queue_name")
            .register();

    private static final Counter SPILLS_TOTAL = Counter.build()
            .name("spillable_queue_spills_total")
            .help("Total number of spill-to-disk batch operations")
            .labelNames("queue_name")
            .register();

    private static final Counter LOADS_TOTAL = Counter.build()
            .name("spillable_queue_loads_total")
            .help("Total number of load-from-disk batch operations")
            .labelNames("queue_name")
            .register();

    private static final Gauge QUEUE_SIZE = Gauge.build()
            .name("spillable_queue_size")
            .help("Current total number of elements in the queue (memory + disk)")
            .labelNames("queue_name")
            .register();

    private static final Gauge SPILL_FILES = Gauge.build()
            .name("spillable_queue_spill_files")
            .help("Current number of spill files on disk")
            .labelNames("queue_name")
            .register();

    /* ──────────────────────── configuration ──────────────────────── */

    private final int                          memoryCapacity;
    private final int                          spillBatchSize;
    private final Path                         spillDir;
    private final ISpillableQueueSerializer<E> ISpillableQueueSerializer;
    private final String                       queueName;

    /* ──────────────────────── metric children (pre-bound label) ──────────────────────── */

    private final Counter.Child offersCounter;
    private final Counter.Child pollsCounter;
    private final Counter.Child spillsCounter;
    private final Counter.Child loadsCounter;
    private final Gauge.Child   sizeGauge;
    private final Gauge.Child   spillFilesGauge;

    /* ──────────────────────── state ──────────────────────── */

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    /**
     * Buffer where writers place new elements.
     * Spilled to disk when its size reaches memoryCapacity.
     */
    private Deque<E> writeBuffer = new ArrayDeque<>();

    /**
     * Buffer from which the reader takes elements.
     * Populated from spill files or by swapping the writeBuffer.
     */
    private Deque<E> readBuffer = new ArrayDeque<>();

    /** Ordered queue of spill file paths (oldest first). */
    private final Deque<Path> spillFiles = new ArrayDeque<>();

    /** Monotonically increasing id for spill file naming. */
    private final AtomicLong spillSeq = new AtomicLong();

    /** Total logical size: readBuffer + writeBuffer + all spill files. */
    private long totalSize;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /* ──────────────────────── construction ──────────────────────── */

    /**
     * @param memoryCapacity  max elements held in the write buffer before spilling
     * @param spillBatchSize  how many elements to write per spill file
     * @param spillDir        directory for temporary spill files
     */
    public SpillableQueueImpl(String aQueueName, int memoryCapacity, int spillBatchSize, Path spillDir) {
        this(aQueueName, memoryCapacity, spillBatchSize, spillDir, new SpillableQueueSerializerJavaSerImpl<>());
    }

    public SpillableQueueImpl(String aQueueName, int memoryCapacity, int spillBatchSize, Path spillDir,
                              ISpillableQueueSerializer<E> ISpillableQueueSerializer) {
        if (memoryCapacity < 1)  throw new IllegalArgumentException("memoryCapacity must be >= 1");
        if (spillBatchSize < 1)  throw new IllegalArgumentException("spillBatchSize must be >= 1");

        this.queueName                 = aQueueName;
        this.memoryCapacity            = memoryCapacity;
        this.spillBatchSize            = spillBatchSize;
        this.spillDir                  = spillDir;
        this.ISpillableQueueSerializer = ISpillableQueueSerializer;

        this.offersCounter   = OFFERS_TOTAL.labels(aQueueName);
        this.pollsCounter    = POLLS_TOTAL.labels(aQueueName);
        this.spillsCounter   = SPILLS_TOTAL.labels(aQueueName);
        this.loadsCounter    = LOADS_TOTAL.labels(aQueueName);
        this.sizeGauge       = QUEUE_SIZE.labels(aQueueName);
        this.spillFilesGauge = SPILL_FILES.labels(aQueueName);

        try {
            Files.createDirectories(spillDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create spill directory: " + spillDir, e);
        }
    }

    /* ──────────────────────── writer API ──────────────────────── */

    /**
     * Adds an element. If the write buffer is full the oldest batch is spilled
     * to disk to make room. This call never blocks waiting for the reader.
     */
    public void offer(E element) {
        if (closed.get()) throw new IllegalStateException("Queue is closed");

        lock.lock();
        try {
            if (writeBuffer.size() >= memoryCapacity) {
                spillToDisk();
            }

            writeBuffer.addLast(element);
            totalSize++;
            offersCounter.inc();
            sizeGauge.inc();
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /* ──────────────────────── reader API ──────────────────────── */

    /**
     * Retrieves and removes the head element, or {@code null} if empty.
     * Non-blocking.
     */
    public E poll() {
        lock.lock();
        try {
            return pollInternal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves and removes the head element, blocking until one is available
     * or the queue is closed.
     *
     * @return the element, or {@code null} if the queue was closed while waiting
     */
    @Override
    public E take() throws InterruptedException {
        lock.lock();
        try {
            E item;
            while ((item = pollInternal()) == null) {
                if (closed.get()) return null;
                notEmpty.await();
            }
            return item;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes up to {@code maxElements} elements and returns them as a list.
     * Non-blocking: returns immediately, possibly with an empty list.
     */
    @Override
    public List<E> drainTo(int maxElements) {
        lock.lock();
        try {
            return drainInternal(maxElements);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes up to {@code maxElements} elements and returns them as a list.
     * Blocks until at least one element is available or {@code timeout} elapses.
     * Returns an empty list if the queue is still empty after the timeout.
     */
    @Override
    public List<E> drainTo(int maxElements, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        lock.lock();
        try {
            while (totalSize == 0 && !closed.get()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) break;
                notEmpty.awaitNanos(remaining);
            }
            return drainInternal(maxElements);
        } finally {
            lock.unlock();
        }
    }

    /** Must be called under lock. */
    private List<E> drainInternal(int maxElements) {
        List<E> result = new ArrayList<>(Math.min(maxElements, 256));
        for (int i = 0; i < maxElements; i++) {
            E elem = pollInternal();
            if (elem == null) break;
            result.add(elem);
        }
        return result;
    }

    /* ──────────────────────── introspection ──────────────────────── */

    /** Total number of elements across memory + disk. */
    public long size() {
        lock.lock();
        try {
            return totalSize;
        } finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    /** Elements currently in RAM (both buffers). */
    public int memorySize() {
        lock.lock();
        try {
            return readBuffer.size() + writeBuffer.size();
        } finally {
            lock.unlock();
        }
    }

    /** Number of spill files on disk. */
    public int spillFileCount() {
        lock.lock();
        try {
            return spillFiles.size();
        } finally {
            lock.unlock();
        }
    }

    /* ──────────────────────── lifecycle ──────────────────────── */

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;

        lock.lock();
        try {
            notEmpty.signalAll();
            for (Path p : spillFiles) {
                try { Files.deleteIfExists(p); } catch (IOException ignored) {}
            }
            spillFiles.clear();
        } finally {
            lock.unlock();
        }
    }

    /* ──────────────────────── internals ──────────────────────── */

    /**
     * Must be called under lock.
     * Drain order: readBuffer → disk spill files → writeBuffer.
     * This guarantees FIFO across all three tiers.
     */
    private E pollInternal() {
        // 1. Read buffer has the oldest ready-to-read data
        if (!readBuffer.isEmpty()) {
            totalSize--;
            pollsCounter.inc();
            sizeGauge.dec();
            return readBuffer.pollFirst();
        }

        // 2. Load the oldest spill file into readBuffer
        if (!spillFiles.isEmpty()) {
            loadFromDisk();
            if (!readBuffer.isEmpty()) {
                totalSize--;
                pollsCounter.inc();
                sizeGauge.dec();
                return readBuffer.pollFirst();
            }
        }

        // 3. No spill files — swap writeBuffer into readBuffer (O(1))
        if (!writeBuffer.isEmpty()) {
            Deque<E> tmp = readBuffer;
            readBuffer = writeBuffer;
            writeBuffer = tmp;  // reuse the (empty) old readBuffer

            totalSize--;
            pollsCounter.inc();
            sizeGauge.dec();
            return readBuffer.pollFirst();
        }

        return null;
    }

    /**
     * Writes the oldest {@code spillBatchSize} elements from writeBuffer to a file.
     * Must be called under lock.
     */
    private void spillToDisk() {
        int count = Math.min(spillBatchSize, writeBuffer.size());
        if (count == 0) return;

        Path spillFile = spillDir.resolve("spill-" + spillSeq.getAndIncrement() + ".dat");

        try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(spillFile))) {
            for (int i = 0; i < count; i++) {
                E elem = writeBuffer.pollFirst();
                byte[] data = ISpillableQueueSerializer.serialize(elem);
                writeInt(os, data.length);
                os.write(data);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to spill to disk: " + spillFile, e);
        }

        spillFiles.addLast(spillFile);
        spillsCounter.inc();
        spillFilesGauge.inc();
    }

    /**
     * Reads the oldest spill file into readBuffer.
     * Must be called under lock.
     */
    private void loadFromDisk() {
        Path spillFile = spillFiles.pollFirst();
        if (spillFile == null) return;
        loadsCounter.inc();
        spillFilesGauge.dec();

        try (InputStream is = new BufferedInputStream(Files.newInputStream(spillFile))) {
            while (true) {
                int len = readInt(is);
                if (len < 0) break;

                byte[] data = is.readNBytes(len);
                if (data.length < len) {
                    throw new IOException("Truncated spill file: " + spillFile);
                }
                E elem = ISpillableQueueSerializer.deserialize(data);
                readBuffer.addLast(elem);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read spill file: " + spillFile, e);
        } finally {
            try { Files.deleteIfExists(spillFile); } catch (IOException ignored) {}
        }
    }

    /* ──────────── framing helpers ──────────── */

    private static void writeInt(OutputStream os, int value) throws IOException {
        os.write((value >>> 24) & 0xFF);
        os.write((value >>> 16) & 0xFF);
        os.write((value >>>  8) & 0xFF);
        os.write( value         & 0xFF);
    }

    private static int readInt(InputStream is) throws IOException {
        int b0 = is.read();
        if (b0 < 0) return -1;
        int b1 = is.read();
        int b2 = is.read();
        int b3 = is.read();
        if (b1 < 0 || b2 < 0 || b3 < 0) {
            throw new IOException("Unexpected EOF reading length prefix");
        }
        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
    }
}

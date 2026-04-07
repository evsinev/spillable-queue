package com.payneteasy.spillable_queue;

import com.payneteasy.spillable_queue.impl.SpillableQueueImpl;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class SpillableQueueImplTest {

    @TempDir
    Path spillDir;

    // ───────────────────── basic operations ─────────────────────

    @Test
    @DisplayName("offer and poll single element")
    void offerAndPollSingle() {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1", 10, 5, spillDir)) {
            queue.offer("hello");
            assertEquals(1, queue.size());
            assertEquals("hello", queue.poll());
            assertEquals(0, queue.size());
        }
    }

    @Test
    @DisplayName("poll returns null on empty queue")
    void pollEmptyReturnsNull() {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1",10, 5, spillDir)) {
            assertNull(queue.poll());
        }
    }

    @Test
    @DisplayName("FIFO ordering within memory")
    void fifoInMemory() {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1",100, 50, spillDir)) {
            for (int i = 0; i < 20; i++) {
                queue.offer("msg-" + i);
            }
            for (int i = 0; i < 20; i++) {
                assertEquals("msg-" + i, queue.poll());
            }
        }
    }

    // ───────────────────── spill to disk ─────────────────────

    @Test
    @DisplayName("spills to disk when memory is full")
    void spillsToDisk() {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1",5, 3, spillDir)) {
            // Fill memory (5) then trigger spill
            for (int i = 0; i < 10; i++) {
                queue.offer("item-" + i);
            }
            assertEquals(10, queue.size());
            assertTrue(queue.spillFileCount() > 0, "Expected spill files on disk");
        }
    }

    @Test
    @DisplayName("FIFO ordering across memory and disk spill")
    void fifoAcrossSpill() {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1",3, 2, spillDir)) {
            for (int i = 0; i < 12; i++) {
                queue.offer("msg-" + i);
            }
            for (int i = 0; i < 12; i++) {
                assertEquals("msg-" + i, queue.poll(),
                             "FIFO violation at position " + i);
            }
            assertTrue(queue.isEmpty());
        }
    }

    @Test
    @DisplayName("spill files are cleaned up after reading")
    void spillFilesCleanedUp() {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1",3, 2, spillDir)) {
            for (int i = 0; i < 20; i++) {
                queue.offer("x");
            }
            int spillsBefore = queue.spillFileCount();
            assertTrue(spillsBefore > 0);

            // Drain all
            while (queue.poll() != null) { /* drain */ }

            assertEquals(0, queue.spillFileCount());
        }
    }

    // ───────────────────── blocking take ─────────────────────

    @Test
    @DisplayName("take() blocks until element available")
    void takeBlocks() throws Exception {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1",10, 5, spillDir)) {
            CountDownLatch started = new CountDownLatch(1);
            Future<String> future = Executors.newSingleThreadExecutor().submit(() -> {
                started.countDown();
                return queue.take();
            });

            started.await();
            Thread.sleep(100); // let it block
            assertFalse(future.isDone(), "take() should be blocking");

            queue.offer("wakeup");
            assertEquals("wakeup", future.get(2, TimeUnit.SECONDS));
        }
    }

    @Test
    @DisplayName("take() returns null when queue is closed")
    void takeReturnsNullOnClose() throws Exception {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1",10, 5, spillDir)) {
            Future<String> future = Executors.newSingleThreadExecutor().submit(queue::take);
            Thread.sleep(100);
            queue.close();
            assertNull(future.get(2, TimeUnit.SECONDS));
        }
    }

    // ───────────────────── concurrency ─────────────────────

    @Test
    @DisplayName("many writers / single reader — no data loss")
    void manyWritersSingleReader() throws Exception {
        final int numWriters = 8;
        final int msgsPerWriter = 10_000;
        final int totalMsgs = numWriters * msgsPerWriter;

        try (var queue = new SpillableQueueImpl<Integer>("test-queue-1",500, 100, spillDir)) {
            CountDownLatch writersFinished = new CountDownLatch(numWriters);
            ExecutorService writerPool = Executors.newFixedThreadPool(numWriters);
            AtomicInteger writeErrors = new AtomicInteger();

            for (int w = 0; w < numWriters; w++) {
                final int writerId = w;
                writerPool.submit(() -> {
                    try {
                        for (int i = 0; i < msgsPerWriter; i++) {
                            queue.offer(writerId * msgsPerWriter + i);
                        }
                    } catch (Exception e) {
                        writeErrors.incrementAndGet();
                    } finally {
                        writersFinished.countDown();
                    }
                });
            }

            // Single reader
            Set<Integer> seen = ConcurrentHashMap.newKeySet();
            AtomicLong readCount = new AtomicLong();

            Thread reader = new Thread(() -> {
                try {
                    while (true) {
                        Integer item = queue.take();
                        if (item == null) break;
                        seen.add(item);
                        readCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            reader.start();

            writersFinished.await();
            writerPool.shutdown();
            writerPool.awaitTermination(10, TimeUnit.SECONDS);

            // Wait for reader to drain
            while (queue.size() > 0) {
                Thread.sleep(20);
            }
            queue.close();
            reader.join(5000);

            assertEquals(0, writeErrors.get(), "Writer errors");
            assertEquals(totalMsgs, readCount.get(), "Total messages read");
            assertEquals(totalMsgs, seen.size(), "Unique messages (no duplicates)");
        }
    }

    @Test
    @DisplayName("concurrent writers don't corrupt queue state")
    void concurrentWritersNoCorruption() throws Exception {
        final int numWriters = 16;
        final int msgsPerWriter = 5_000;

        try (var queue = new SpillableQueueImpl<String>("test-queue-1",100, 50, spillDir)) {
            ExecutorService pool = Executors.newFixedThreadPool(numWriters);
            CountDownLatch latch = new CountDownLatch(numWriters);

            for (int w = 0; w < numWriters; w++) {
                final int wid = w;
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < msgsPerWriter; i++) {
                            queue.offer("w" + wid + "-" + i);
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            pool.shutdown();

            assertEquals(numWriters * msgsPerWriter, queue.size());

            // Drain and count
            int count = 0;
            while (queue.poll() != null) count++;
            assertEquals(numWriters * msgsPerWriter, count);
        }
    }

    // ───────────────────── edge cases ─────────────────────

    @Test
    @DisplayName("offer after close throws")
    void offerAfterCloseThrows() {
        var queue = new SpillableQueueImpl<String>("test-queue-1",10, 5, spillDir);
        queue.close();
        assertThrows(IllegalStateException.class, () -> queue.offer("nope"));
    }

    @Test
    @DisplayName("invalid constructor args rejected")
    void invalidConstructorArgs() {
        assertThrows(IllegalArgumentException.class,
                     () -> new SpillableQueueImpl<String>("test-queue-1",0, 5, spillDir));
        assertThrows(IllegalArgumentException.class,
                     () -> new SpillableQueueImpl<String>("test-queue-1",10, 0, spillDir));
    }

    @Test
    @DisplayName("works with various Serializable types")
    void variousTypes() {
        try (var intQueue = new SpillableQueueImpl<Integer>("test-queue-1",3, 2, spillDir.resolve("int"))) {
            for (int i = 0; i < 10; i++) intQueue.offer(i);
            for (int i = 0; i < 10; i++) assertEquals(i, intQueue.poll());
        }

        try (var dblQueue = new SpillableQueueImpl<Double>("test-queue-1",3, 2, spillDir.resolve("dbl"))) {
            dblQueue.offer(3.14);
            dblQueue.offer(2.72);
            assertEquals(3.14, dblQueue.poll());
            assertEquals(2.72, dblQueue.poll());
        }
    }

    @Test
    @DisplayName("large batch spill and reload")
    void largeBatch() {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1",10, 1000, spillDir)) {
            for (int i = 0; i < 5000; i++) {
                queue.offer("large-" + i);
            }
            for (int i = 0; i < 5000; i++) {
                assertEquals("large-" + i, queue.poll());
            }
        }
    }

    // ───────────────────── drainTo ─────────────────────

    @Test
    @DisplayName("drainTo() returns empty list on empty queue")
    void drainToEmptyQueue() {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1", 10, 5, spillDir)) {
            List<String> result = queue.drainTo(10);
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }
    }

    @Test
    @DisplayName("drainTo() returns up to maxElements, leaves the rest")
    void drainToMaxElements() {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1", 20, 10, spillDir)) {
            for (int i = 0; i < 10; i++) queue.offer("msg-" + i);

            List<String> result = queue.drainTo(4);
            assertEquals(4, result.size());
            assertEquals(List.of("msg-0", "msg-1", "msg-2", "msg-3"), result);
            assertEquals(6, queue.size());
        }
    }

    @Test
    @DisplayName("drainTo() preserves FIFO order across spill")
    void drainToFifoAcrossSpill() {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1", 3, 2, spillDir)) {
            for (int i = 0; i < 12; i++) queue.offer("msg-" + i);

            List<String> all = queue.drainTo(100);
            assertEquals(12, all.size());
            for (int i = 0; i < 12; i++) {
                assertEquals("msg-" + i, all.get(i), "FIFO violation at " + i);
            }
        }
    }

    @Test
    @DisplayName("drainTo(timeout) returns empty list when timeout elapses with no elements")
    void drainToTimeoutEmpty() throws Exception {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1", 10, 5, spillDir)) {
            long start = System.nanoTime();
            List<String> result = queue.drainTo(10, Duration.ofMillis(100));
            long elapsed = System.nanoTime() - start;

            assertTrue(result.isEmpty());
            assertTrue(elapsed >= 100_000_000L, "Should have waited ~100 ms");
        }
    }

    @Test
    @DisplayName("drainTo(timeout) returns elements offered before timeout")
    void drainToTimeoutReceivesElements() throws Exception {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1", 10, 5, spillDir)) {
            Executors.newSingleThreadScheduledExecutor().schedule(() -> {
                queue.offer("a");
                queue.offer("b");
                queue.offer("c");
            }, 50, TimeUnit.MILLISECONDS);

            List<String> result = queue.drainTo(10, Duration.ofMillis(500));
            assertFalse(result.isEmpty());
            assertTrue(result.contains("a"));
        }
    }

    @Test
    @DisplayName("drainTo(timeout) respects maxElements limit")
    void drainToTimeoutRespectsMax() throws Exception {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1", 10, 5, spillDir)) {
            for (int i = 0; i < 8; i++) queue.offer("x-" + i);

            List<String> result = queue.drainTo(3, Duration.ofMillis(100));
            assertEquals(3, result.size());
            assertEquals(5, queue.size());
        }
    }

    @Test
    @DisplayName("drainTo(timeout) returns immediately when elements already present")
    void drainToTimeoutImmediate() throws Exception {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1", 10, 5, spillDir)) {
            queue.offer("ready");

            long start = System.nanoTime();
            List<String> result = queue.drainTo(10, Duration.ofSeconds(10));
            long elapsed = System.nanoTime() - start;

            assertEquals(List.of("ready"), result);
            assertTrue(elapsed < 1_000_000_000L, "Should return well before 1 s");
        }
    }

    // ───────────────────── introspection ─────────────────────

    @Test
    @DisplayName("size, memorySize, spillFileCount are consistent")
    void sizeIntrospection() {
        try (var queue = new SpillableQueueImpl<String>("test-queue-1",5, 3, spillDir)) {
            assertEquals(0, queue.size());
            assertTrue(queue.isEmpty());
            assertEquals(0, queue.memorySize());
            assertEquals(0, queue.spillFileCount());

            for (int i = 0; i < 5; i++) queue.offer("m" + i);
            assertEquals(5, queue.size());
            assertEquals(5, queue.memorySize());
            assertEquals(0, queue.spillFileCount());

            // Trigger spill
            queue.offer("overflow");
            assertEquals(6, queue.size());
            assertTrue(queue.spillFileCount() > 0 || queue.memorySize() <= 5);
        }
    }
}

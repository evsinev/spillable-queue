package com.payneteasy.spillable_queue;

import com.payneteasy.spillable_queue.impl.SpillableQueueImpl;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Demonstration: 8 writer threads push 100 000 messages each into a
 * SpillableQueue with a 1 000-element memory cap; a single reader drains
 * everything and verifies the total count.
 */
public class SpillableQueueDemo {

    private static final int NUM_WRITERS       = 8;
    private static final int MESSAGES_PER_WRITER = 100_000;
    private static final int TOTAL_MESSAGES     = NUM_WRITERS * MESSAGES_PER_WRITER;

    private static final int MEMORY_CAPACITY   = 1_000;
    private static final int SPILL_BATCH_SIZE  = 500;

    public static void main(String[] args) throws Exception {

        Path spillDir = Files.createTempDirectory("spillable-queue-demo-");
        System.out.printf("Spill dir : %s%n", spillDir);
        System.out.printf("Writers   : %d × %,d msgs = %,d total%n",
                NUM_WRITERS, MESSAGES_PER_WRITER, TOTAL_MESSAGES);
        System.out.printf("Memory cap: %,d elements, spill batch: %,d%n%n",
                MEMORY_CAPACITY, SPILL_BATCH_SIZE);

        try (SpillableQueueImpl<String> queue =
                     new SpillableQueueImpl<>("test-queue-1", MEMORY_CAPACITY, SPILL_BATCH_SIZE, spillDir)) {

            CountDownLatch writersFinished = new CountDownLatch(NUM_WRITERS);
            ExecutorService writerPool = Executors.newFixedThreadPool(NUM_WRITERS);
            AtomicInteger writeErrors = new AtomicInteger();

            long t0 = System.nanoTime();

            // ── launch writers ────────────────────────────────────────
            for (int w = 0; w < NUM_WRITERS; w++) {
                final int writerId = w;
                writerPool.submit(() -> {
                    try {
                        for (int i = 0; i < MESSAGES_PER_WRITER; i++) {
                            queue.offer("writer-" + writerId + "-msg-" + i);
                        }
                    } catch (Exception e) {
                        writeErrors.incrementAndGet();
                        e.printStackTrace();
                    } finally {
                        writersFinished.countDown();
                    }
                });
            }

            // ── single reader ─────────────────────────────────────────
            AtomicLong readCount = new AtomicLong();
            AtomicLong maxSpillFiles = new AtomicLong();

            Thread reader = new Thread(() -> {
                try {
                    while (true) {
                        String item = queue.take();
                        if (item == null) break;  // queue closed

                        long count = readCount.incrementAndGet();
                        long spills = queue.spillFileCount();
                        maxSpillFiles.accumulateAndGet(spills, Math::max);

                        if (count % 100_000 == 0) {
                            System.out.printf("  [reader] %,d read | queue size: %,d | "
                                            + "memory: %,d | spill files: %d%n",
                                    count, queue.size(), queue.memorySize(), spills);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "reader");
            reader.start();

            // Wait for all writers to finish, then close (signals reader to stop)
            writersFinished.await();
            writerPool.shutdown();
            writerPool.awaitTermination(10, TimeUnit.SECONDS);

            // Drain remaining items — reader might still be busy
            while (queue.size() > 0) {
                Thread.sleep(50);
            }
            queue.close();
            reader.join(5000);

            long elapsed = System.nanoTime() - t0;

            // ── report ────────────────────────────────────────────────
            System.out.println();
            System.out.println("═══════════════════════════════════════════");
            System.out.printf("Total written  : %,d%n", TOTAL_MESSAGES);
            System.out.printf("Total read     : %,d%n", readCount.get());
            System.out.printf("Write errors   : %d%n", writeErrors.get());
            System.out.printf("Max spill files: %d%n", maxSpillFiles.get());
            System.out.printf("Elapsed        : %.2f s%n", elapsed / 1e9);
            System.out.printf("Throughput     : %,.0f msg/s%n",
                    TOTAL_MESSAGES / (elapsed / 1e9));
            System.out.println("═══════════════════════════════════════════");

            if (readCount.get() == TOTAL_MESSAGES && writeErrors.get() == 0) {
                System.out.println("✓ SUCCESS — all messages delivered");
            } else {
                System.out.println("✗ FAILURE — count mismatch or errors!");
                System.exit(1);
            }
        }
    }
}

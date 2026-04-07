# SpillableQueue

Thread-safe bounded queue for Java 21 that stores elements in memory up to a configurable threshold, then spills overflow batches to disk. Designed for the **many-writers / single-reader** pattern.

## How it works

```
Writers ──► [ writeBuffer (bounded) ]
                     │ overflow → spill batch to disk
                     ▼
            [ disk spill files (FIFO) ]
                     │ refill when readBuffer empty
                     ▼
            [ readBuffer ] ──► Reader (poll / take)
```

Two internal buffers maintain strict FIFO ordering even across disk spills:

- **writeBuffer** — writers append here. When it reaches `memoryCapacity`, a batch is serialized to a spill file.
- **readBuffer** — the reader drains this first. When empty, the oldest spill file is loaded into it. If no spill files exist, the writeBuffer is swapped in as the new readBuffer (O(1)).

## Quick start

```java
Path spillDir = Files.createTempDirectory("spill-");

try (ISpillableQueue<String> queue =
        new SpillableQueueImpl<>("my-queue", 10_000, 5_000, spillDir)) {

    // Writers (any thread)
    queue.offer("hello");

    // Reader (single thread)
    String item = queue.take();  // blocks until available
}
```

## API

| Method | Description |
|--------|-------------|
| `offer(E)` | Add an element. Spills oldest batch to disk if write buffer is full. Never blocks. |
| `poll()` | Retrieve and remove head element, or `null` if empty. Non-blocking. |
| `take()` | Retrieve and remove head element, blocking until available. Returns `null` if queue is closed. |
| `drainTo(int)` | Remove up to N elements and return them as a list. Non-blocking. |
| `drainTo(int, Duration)` | Same, but blocks until at least one element is available or timeout elapses. |
| `size()` | Total element count across memory + disk. |
| `close()` | Close the queue and delete spill files. |

## Constructor

```java
new SpillableQueueImpl<>(
    String queueName,
    int    memoryCapacity,   // max elements in write buffer before spilling
    int    spillBatchSize,   // elements per spill file
    Path   spillDir          // directory for temporary spill files
)
```

An optional fifth argument accepts a custom `ISpillableQueueSerializer<E>` (default: Java serialization).

## Custom serializer

```java
public class MySerializer implements ISpillableQueueSerializer<MyEvent> {
    @Override public byte[] serialize(MyEvent e) { /* ... */ }
    @Override public MyEvent deserialize(byte[] data) { /* ... */ }
}

new SpillableQueueImpl<>("q", 10_000, 5_000, spillDir, new MySerializer());
```

Drop-in replacements with Kryo, Protobuf, or Jackson are straightforward to implement.

## Build & test

```bash
# build
mvn clean package

# run tests
mvn test

# run demo (8 writers × 100 000 msgs, 1 reader)
mvn exec:java -Dexec.mainClass="com.payneteasy.spillable_queue.SpillableQueueDemo"
```

## Project structure

```
src/
├── main/java/com/payneteasy/spillable_queue/
│   ├── ISpillableQueue.java                        # public queue interface
│   ├── ISpillableQueueSerializer.java              # pluggable serializer interface
│   └── impl/
│       ├── SpillableQueueImpl.java                 # main implementation
│       └── SpillableQueueSerializerJavaSerImpl.java # default Java serialization
└── test/java/com/payneteasy/spillable_queue/
    ├── SpillableQueueImplTest.java                 # unit / integration tests
    └── SpillableQueueDemo.java                     # demo with 8 writers + 1 reader
```

## Metrics (Prometheus)

Dependency: `io.prometheus:simpleclient_common:0.16.0`.

All metrics carry the label `queue_name` (value from the `queueName` constructor argument).

| Metric | Type | Description |
|--------|------|-------------|
| `spillable_queue_offered_total` | Counter | Elements added via `offer()` |
| `spillable_queue_polled_total` | Counter | Elements removed from the queue |
| `spillable_queue_spills_total` | Counter | Spill-to-disk batch operations |
| `spillable_queue_loads_total` | Counter | Load-from-disk batch operations |
| `spillable_queue_size` | Gauge | Current total elements (memory + disk) |
| `spillable_queue_spill_files` | Gauge | Current number of spill files on disk |
| `spillable_queue_offer_duration_seconds` | Histogram | Latency of `offer()` including any spill-to-disk |
| `spillable_queue_poll_duration_seconds` | Histogram | Latency of individual dequeue operations excluding blocking wait |

Histogram buckets: 1µs, 10µs, 100µs, 1ms, 10ms, 100ms, 1s.

Expose via your preferred Prometheus servlet or `TextFormat.write004()` from `simpleclient_common`.

## Design notes

- Concurrency: `ReentrantLock` + `Condition`, no `synchronized`
- Spill file format: length-prefixed framing (4-byte big-endian length + payload)
- Spill files are deleted immediately after being read back into memory
- Elements must implement `java.io.Serializable` (required by `ISpillableQueueSerializer`)

## Known limitations / backlog

- `offer()` never blocks — it always spills to disk instead of applying back-pressure on writers
- No bounded disk usage (no cap on total spill file size)
- No compression for spill files (snappy/lz4)
- No memory-mapped file I/O
- No graceful shutdown with drain
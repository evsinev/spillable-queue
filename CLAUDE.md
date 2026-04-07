# SpillableQueue

## Что это

Thread-safe bounded queue на Java 21, которая хранит элементы в памяти до порога `memoryCapacity`, а при переполнении сбрасывает (spill) старейшие батчи на диск. Рассчитана на паттерн **many-writers / single-reader**.

## Архитектура

```
Writers ──► [ writeBuffer (ArrayDeque, bounded) ]
                     │ overflow
                     ▼
            [ disk spill files (FIFO) ]
                     │ refill when readBuffer empty
                     ▼
            [ readBuffer ] ──► Reader (poll / take)
```

Ключевые классы:
- `SpillableQueueImpl<E>` — основная очередь, lock-based concurrency
- `ISpillableQueue<E>` — публичный интерфейс очереди
- `ISpillableQueueSerializer<E>` — интерфейс сериализации для spill на диск
- `SpillableQueueSerializerJavaSerImpl<E>` — дефолтная реализация через ObjectOutputStream
- `SpillableQueueDemo` — демо с 8 писателями и 1 читателем

## Сборка и запуск

```bash
# сборка
mvn clean package

# запуск тестов
mvn test

# запуск демо
mvn exec:java -Dexec.mainClass="com.payneteasy.spillable_queue.SpillableQueueDemo"
```

## Структура проекта

```
src/
├── main/java/com/payneteasy/spillable_queue/
│   ├── ISpillableQueue.java                         # публичный интерфейс очереди
│   ├── ISpillableQueueSerializer.java               # интерфейс сериализации
│   └── impl/
│       ├── SpillableQueueImpl.java                  # основная очередь
│       └── SpillableQueueSerializerJavaSerImpl.java # дефолтный сериализатор
└── test/java/com/payneteasy/spillable_queue/
    ├── SpillableQueueImplTest.java                  # юнит-тесты
    └── SpillableQueueDemo.java                      # демо
```

## Метрики (Prometheus)

Зависимость: `io.prometheus:simpleclient_common:0.16.0`.

Все метрики имеют label `queue_name` (значение из поля `queueName`).
Метрики объявлены `static` в `SpillableQueueImpl`; `Counter.Child` / `Gauge.Child` с привязанным label хранятся в полях инстанса.

| Метрика | Тип | Что считает |
|---|---|---|
| `spillable_queue_offered_total` | Counter | элементы, добавленные через `offer()` |
| `spillable_queue_polled_total` | Counter | элементы, извлечённые из очереди |
| `spillable_queue_spills_total` | Counter | батч-операции записи на диск |
| `spillable_queue_loads_total` | Counter | батч-операции загрузки с диска |
| `spillable_queue_size` | Gauge | текущий размер очереди (память + диск) |
| `spillable_queue_spill_files` | Gauge | текущее количество spill-файлов на диске |
| `spillable_queue_offer_duration_seconds` | Histogram | latency `offer()` включая возможный spill на диск |
| `spillable_queue_poll_duration_seconds` | Histogram | latency одного извлечения элемента без учёта блокирующего ожидания |

Buckets гистограмм: 1µs, 10µs, 100µs, 1ms, 10ms, 100ms, 1s.

## Conventions

- Java 21, без preview features
- Потокобезопасность: `ReentrantLock` + `Condition`, не `synchronized`
- Spill файлы: length-prefixed framing (4 байта big-endian длина + payload)
- Spill директория задаётся при создании, файлы удаляются после прочтения
- Сериализатор подключаемый через `Serializer<E>` интерфейс
- Тесты: JUnit 5, `@TempDir` для spill-директорий, без моков — всё интеграционное
- Именование: camelCase для методов/полей, PascalCase для классов

## Что НЕ реализовано (backlog)

- Back-pressure: `offer()` сейчас не блокирует писателей, всегда spill'ит на диск
- Compression: сжатие spill файлов (snappy/lz4)
- Memory-mapped files вместо обычного I/O
- Graceful shutdown с drain
- Bounded disk usage (лимит на суммарный размер spill файлов)

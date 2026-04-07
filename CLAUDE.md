# SpillableQueue

## Что это

Thread-safe bounded queue на Java 21, которая хранит элементы в памяти до порога `memoryCapacity`, а при переполнении сбрасывает (spill) старейшие батчи на диск. Рассчитана на паттерн **many-writers / single-reader**.

## Архитектура

```
Writers ──► [ memoryBuffer (ArrayDeque, bounded) ]
                     │ overflow
                     ▼
            [ disk spill files (FIFO) ]
                     │ refill when memory empty
                     ▼
              Reader (poll / take)
```

Ключевые классы:
- `SpillableQueue<E>` — основная очередь, lock-based concurrency
- `Serializer<E>` — интерфейс сериализации для spill на диск
- `JavaSerializer<E>` — дефолтная реализация через ObjectOutputStream
- `SpillableQueueDemo` — демо с 8 писателями и 1 читателем

## Сборка и запуск

```bash
# сборка
mvn clean package

# запуск тестов
mvn test

# запуск демо
mvn exec:java -Dexec.mainClass="com.queue.SpillableQueueDemo"
```

## Структура проекта

```
src/
├── main/java/com/queue/
│   ├── SpillableQueue.java      # основная очередь
│   ├── Serializer.java          # интерфейс сериализации
│   ├── JavaSerializer.java      # дефолтный сериализатор
│   └── SpillableQueueDemo.java  # демо
└── test/java/com/queue/
    └── SpillableQueueTest.java  # юнит-тесты
```

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
- Batch poll: `drainTo(Collection, maxElements)` для читателя
- Метрики: счётчики spill/load операций, latency
- Compression: сжатие spill файлов (snappy/lz4)
- Memory-mapped files вместо обычного I/O
- Graceful shutdown с drain
- Bounded disk usage (лимит на суммарный размер spill файлов)

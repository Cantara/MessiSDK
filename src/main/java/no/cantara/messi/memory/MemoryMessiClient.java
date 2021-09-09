package no.cantara.messi.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiNoSuchExternalIdException;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Optional.ofNullable;

public class MemoryMessiClient implements MessiClient {

    final Map<String, MemoryMessiTopic> topicByName = new ConcurrentHashMap<>();
    final AtomicBoolean closed = new AtomicBoolean(false);
    final List<MemoryMessiProducer> producers = new CopyOnWriteArrayList<>();
    final List<MemoryMessiConsumer> consumers = new CopyOnWriteArrayList<>();

    @Override
    public MemoryMessiProducer producer(String topicName) {
        if (isClosed()) {
            throw new MessiClosedException();
        }
        MemoryMessiProducer producer = new MemoryMessiProducer(topicByName.computeIfAbsent(topicName, MemoryMessiTopic::new), producers::remove);
        this.producers.add(producer);
        return producer;
    }

    @Override
    public MessiConsumer consumer(String topic, MessiCursor cursor) {
        if (isClosed()) {
            throw new MessiClosedException();
        }
        MemoryMessiConsumer consumer = new MemoryMessiConsumer(
                topicByName.computeIfAbsent(topic, MemoryMessiTopic::new),
                (MemoryMessiCursor) cursor,
                consumers::remove
        );
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public MessiCursor cursorOf(String topicName, ULID.Value ulid, boolean inclusive) {
        return new MemoryMessiCursor(ulid, inclusive, true);
    }

    @Override
    public MessiCursor cursorOf(String topicName, String externalId, boolean inclusive, long approxTimestamp, Duration tolerance) {
        MemoryMessiTopic topic = topicByName.computeIfAbsent(topicName, MemoryMessiTopic::new);
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            ULID.Value lowerBound = MessiULIDUtils.beginningOf(approxTimestamp - tolerance.toMillis());
            ULID.Value upperBound = MessiULIDUtils.beginningOf(approxTimestamp + tolerance.toMillis());
            return ofNullable(topic.ulidOf(externalId, lowerBound, upperBound))
                    .map(ulid -> new MemoryMessiCursor(ulid, inclusive, true))
                    .orElseThrow(() -> new MessiNoSuchExternalIdException(String.format("Position not found: %s", externalId)));
        } finally {
            topic.unlock();
        }
    }

    @Override
    public MessiMessage lastMessage(String topicName) throws MessiClosedException {
        if (isClosed()) {
            throw new MessiClosedException();
        }
        MemoryMessiTopic topic = topicByName.computeIfAbsent(topicName, MemoryMessiTopic::new);
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            return topic.lastMessage();
        } finally {
            topic.unlock();
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public MemoryMessiMetadataClient metadata(String topic) {
        return new MemoryMessiMetadataClient(topic);
    }

    @Override
    public void close() {
        for (MemoryMessiProducer producer : producers) {
            producer.close();
        }
        producers.clear();
        for (MemoryMessiConsumer consumer : consumers) {
            consumer.close();
        }
        consumers.clear();
        closed.set(true);
    }
}

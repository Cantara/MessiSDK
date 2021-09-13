package no.cantara.messi.memory;

import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.protos.MessiMessage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
    public MemoryMessiCursor.Builder cursorOf() {
        return new MemoryMessiCursor.Builder();
    }

    @Override
    public MessiMessage lastMessage(String topicName, String shardId) throws MessiClosedException {
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

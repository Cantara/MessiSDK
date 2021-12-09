package no.cantara.messi.memory;

import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.protos.MessiMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MemoryMessiClient implements MessiClient {

    final Map<String, MemoryMessiTopic> topicByName = new ConcurrentHashMap<>();
    final AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public MemoryMessiTopic topicOf(String name) {
        return topicByName.computeIfAbsent(name, MemoryMessiTopic::new);
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
        MemoryMessiTopic topic = topicOf(topicName);
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
        for (Map.Entry<String, MemoryMessiTopic> entry : topicByName.entrySet()) {
            MemoryMessiTopic topic = entry.getValue();
            topic.close();
        }
        topicByName.clear();
        closed.set(true);
    }
}

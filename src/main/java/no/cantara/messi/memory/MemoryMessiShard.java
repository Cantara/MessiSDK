package no.cantara.messi.memory;

import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiQueuingConsumer;
import no.cantara.messi.api.MessiShard;
import no.cantara.messi.api.MessiStreamingConsumer;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class MemoryMessiShard implements MessiShard {

    final String shardId;
    final MemoryMessiTopic topic;
    final List<MemoryMessiQueuingConsumer> queueingConsumers = new CopyOnWriteArrayList<>();
    final List<MemoryMessiStreamingConsumer> streamingConsumers = new CopyOnWriteArrayList<>();

    public MemoryMessiShard(String shardId, MemoryMessiTopic topic) {
        this.shardId = shardId;
        this.topic = topic;
    }

    public boolean supportsQueuing() {
        return true;
    }

    public MessiQueuingConsumer queuingConsumer() {
        return new MemoryMessiQueuingConsumer(topic, queueingConsumers::remove);
    }

    public boolean supportsStreaming() {
        return true;
    }

    public MessiStreamingConsumer streamingConsumer(MessiCursor initialPosition) {
        MemoryMessiStreamingConsumer consumer = new MemoryMessiStreamingConsumer(topic, (MemoryMessiCursor) initialPosition, streamingConsumers::remove);
        streamingConsumers.add(consumer);
        return consumer;
    }

    public MemoryMessiCursor.Builder cursorOf() {
        return new MemoryMessiCursor.Builder()
                .shardId(shardId);
    }

    public MemoryMessiCursor cursorOfCheckpoint(String checkpoint) {
        return new MemoryMessiCursor.Builder()
                .shardId(shardId)
                .checkpoint(checkpoint)
                .build();
    }

    public MemoryMessiCursor cursorAt(MessiMessage message) {
        return new MemoryMessiCursor.Builder()
                .shardId(shardId)
                .ulid(MessiULIDUtils.toUlid(message.getUlid()))
                .inclusive(true)
                .build();
    }

    public MemoryMessiCursor cursorAfter(MessiMessage message) {
        return new MemoryMessiCursor.Builder()
                .shardId(shardId)
                .ulid(MessiULIDUtils.toUlid(message.getUlid()))
                .inclusive(false)
                .build();
    }

    public MemoryMessiCursor cursorAtLastMessage() {
        MessiMessage lastMessage = topic.lastMessage();
        if (lastMessage == null) {
            return null;
        }
        return cursorAt(lastMessage);
    }

    public MemoryMessiCursor cursorAfterLastMessage() {
        long now = System.currentTimeMillis();
        MessiMessage lastMessage = topic.lastMessage();
        if (lastMessage == null) {
            return cursorOf()
                    .ulid(MessiULIDUtils.beginningOf(now))
                    .inclusive(true)
                    .build();
        }
        return cursorAfter(lastMessage);
    }

    public MemoryMessiCursor cursorHead() {
        return new MemoryMessiCursor.Builder()
                .shardId(shardId)
                .now()
                .build();
    }

    public MemoryMessiCursor cursorAtTrimHorizon() {
        return new MemoryMessiCursor.Builder()
                .shardId(shardId)
                .oldest()
                .inclusive(true)
                .build();
    }

    @Override
    public void close() {
        for (MemoryMessiQueuingConsumer queueingConsumer : queueingConsumers) {
            queueingConsumer.close();
        }
        queueingConsumers.clear();
        for (MemoryMessiStreamingConsumer streamingConsumer : streamingConsumers) {
            streamingConsumer.close();
        }
        streamingConsumers.clear();
    }
}

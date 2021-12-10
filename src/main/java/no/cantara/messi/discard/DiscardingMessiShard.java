package no.cantara.messi.discard;

import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiShard;
import no.cantara.messi.memory.MemoryMessiCursor;
import no.cantara.messi.protos.MessiMessage;

public class DiscardingMessiShard implements MessiShard {

    final DiscardingMessiTopic topic;

    public DiscardingMessiShard(DiscardingMessiTopic topic) {
        this.topic = topic;
    }

    @Override
    public boolean supportsQueuing() {
        return true;
    }

    @Override
    public DiscardingMessiQueuingConsumer queuingConsumer() {
        return new DiscardingMessiQueuingConsumer(this);
    }

    @Override
    public boolean supportsStreaming() {
        return true;
    }

    @Override
    public DiscardingMessiStreamingConsumer streamingConsumer(MessiCursor initialPosition) {
        return new DiscardingMessiStreamingConsumer(this, topic.name());
    }

    @Override
    public MessiCursor.Builder cursorOf() {
        return null;
    }

    @Override
    public MessiCursor cursorOfCheckpoint(String checkpoint) {
        return null;
    }

    @Override
    public MessiCursor cursorAt(MessiMessage message) {
        return null;
    }

    @Override
    public MessiCursor cursorAfter(MessiMessage message) {
        return null;
    }

    @Override
    public MessiCursor cursorAtLastMessage() throws MessiClosedException {
        return null;
    }

    @Override
    public MessiCursor cursorAfterLastMessage() throws MessiClosedException {
        return null;
    }

    @Override
    public MemoryMessiCursor cursorHead() {
        return null;
    }

    @Override
    public MessiCursor cursorAtTrimHorizon() {
        return null;
    }

    @Override
    public DiscardingMessiTopic topic() {
        return topic;
    }

    @Override
    public void close() {
    }
}

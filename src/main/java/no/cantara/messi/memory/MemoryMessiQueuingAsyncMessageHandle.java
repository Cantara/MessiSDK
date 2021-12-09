package no.cantara.messi.memory;

import no.cantara.messi.api.MessiQueuingAsyncMessageHandle;
import no.cantara.messi.protos.MessiMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class MemoryMessiQueuingAsyncMessageHandle implements MessiQueuingAsyncMessageHandle {
    private final MemoryMessiTopic topic;
    private final MessiMessage messiMessage;

    MemoryMessiQueuingAsyncMessageHandle(MemoryMessiTopic topic, MessiMessage messiMessage) {
        this.topic = topic;
        this.messiMessage = messiMessage;
    }

    @Override
    public MessiMessage message() {
        return messiMessage;
    }

    @Override
    public CompletableFuture<Void> ack() {
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            topic.ackQueue(messiMessage);
            return CompletableFuture.completedFuture(null);
        } finally {
            topic.unlock();
        }
    }
}

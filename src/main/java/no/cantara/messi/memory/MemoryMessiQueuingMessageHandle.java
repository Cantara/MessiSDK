package no.cantara.messi.memory;

import no.cantara.messi.api.MessiQueuingMessageHandle;
import no.cantara.messi.protos.MessiMessage;

import java.util.concurrent.TimeUnit;

class MemoryMessiQueuingMessageHandle implements MessiQueuingMessageHandle {
    private final MemoryMessiTopic topic;
    private final MessiMessage messiMessage;

    MemoryMessiQueuingMessageHandle(MemoryMessiTopic topic, MessiMessage messiMessage) {
        this.topic = topic;
        this.messiMessage = messiMessage;
    }

    @Override
    public MessiMessage message() {
        return messiMessage;
    }

    @Override
    public void ack() {
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            topic.ackQueue(messiMessage);
        } finally {
            topic.unlock();
        }
    }
}

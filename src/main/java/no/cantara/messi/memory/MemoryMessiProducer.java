package no.cantara.messi.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiProducer;
import no.cantara.messi.protos.MessiMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

class MemoryMessiProducer implements MessiProducer {

    final ULID ulid = new ULID();

    final MemoryMessiTopic topic;

    final AtomicBoolean closed = new AtomicBoolean(false);

    final Consumer<MemoryMessiProducer> closeAction;

    MemoryMessiProducer(MemoryMessiTopic topic, Consumer<MemoryMessiProducer> closeAction) {
        this.topic = topic;
        this.closeAction = closeAction;
    }

    @Override
    public String topic() {
        return topic.topic;
    }

    @Override
    public void publish(MessiMessage... messages) throws MessiClosedException {
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            for (MessiMessage message : messages) {
                if (message == null) {
                    throw new NullPointerException("on of the messages was null");
                }
                topic.write(message);
            }
        } finally {
            topic.unlock();
        }
    }

    @Override
    public CompletableFuture<Void> publishAsync(MessiMessage... messages) {
        if (isClosed()) {
            throw new MessiClosedException();
        }
        return CompletableFuture.runAsync(() -> publish(messages));
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closeAction.accept(this);
        closed.set(true);
    }
}

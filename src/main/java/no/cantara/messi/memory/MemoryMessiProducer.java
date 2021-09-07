package no.cantara.messi.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiMessage;
import no.cantara.messi.api.MessiProducer;
import no.cantara.messi.api.MessiULIDUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class MemoryMessiProducer implements MessiProducer {

    final ULID ulid = new ULID();

    final AtomicReference<ULID.Value> prevUlid = new AtomicReference<>(ulid.nextValue());

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
                ULID.Value ulid = message.ulid();
                if (ulid == null) {
                    ulid = MessiULIDUtils.nextMonotonicUlid(this.ulid, prevUlid.get());
                }
                prevUlid.set(ulid);
                topic.write(ulid, message);
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

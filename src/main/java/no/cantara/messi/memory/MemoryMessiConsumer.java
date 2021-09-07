package no.cantara.messi.memory;

import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiMessage;
import no.cantara.messi.api.MessiULIDUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class MemoryMessiConsumer implements MessiConsumer {

    final MemoryMessiTopic topic;
    final Consumer<MemoryMessiConsumer> closeAction;
    final AtomicReference<MemoryMessiCursor> position = new AtomicReference<>();
    final AtomicBoolean closed = new AtomicBoolean(false);

    MemoryMessiConsumer(MemoryMessiTopic topic, MemoryMessiCursor initialPosition, Consumer<MemoryMessiConsumer> closeAction) {
        this.topic = topic;
        this.closeAction = closeAction;
        if (initialPosition == null) {
            initialPosition = new MemoryMessiCursor(MessiULIDUtils.beginningOfTime(), true, true);
        }
        this.position.set(initialPosition);
    }

    @Override
    public String topic() {
        return topic.topic;
    }

    @Override
    public MessiMessage receive(int timeout, TimeUnit unit) throws InterruptedException, MessiClosedException {
        long expireTimeNano = System.nanoTime() + unit.toNanos(timeout);
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            while (!topic.hasNext(position.get())) {
                if (isClosed()) {
                    throw new MessiClosedException();
                }
                long durationNano = expireTimeNano - System.nanoTime();
                if (durationNano <= 0) {
                    return null; // timeout
                }
                topic.awaitProduction(durationNano, TimeUnit.NANOSECONDS);
            }
            MessiMessage message = topic.readNext(position.get());
            position.set(new MemoryMessiCursor(message.ulid(), false, true));
            return message;
        } finally {
            topic.unlock();
        }
    }

    @Override
    public CompletableFuture<MessiMessage> receiveAsync() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return receive(5, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void seek(long timestamp) {
        position.set(new MemoryMessiCursor(MessiULIDUtils.beginningOf(timestamp), true, true));
    }

    @Override
    public String toString() {
        return "MemoryMessiConsumer{" +
                "position=" + position +
                ", closed=" + closed +
                '}';
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closeAction.accept(this);
        closed.set(true);
        if (topic.tryLock()) {
            try {
                topic.signalProduction();
            } finally {
                topic.unlock();
            }
        }
    }
}

package no.cantara.messi.memory;

import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiQueuingAsyncMessageHandle;
import no.cantara.messi.api.MessiQueuingConsumer;
import no.cantara.messi.api.MessiQueuingMessageHandle;
import no.cantara.messi.protos.MessiMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

class MemoryMessiQueuingConsumer implements MessiQueuingConsumer {

    private final MemoryMessiTopic topic;
    final Consumer<MemoryMessiQueuingConsumer> closeAction;
    final AtomicBoolean closed = new AtomicBoolean(false);

    public MemoryMessiQueuingConsumer(MemoryMessiTopic topic, Consumer<MemoryMessiQueuingConsumer> closeAction) {
        this.topic = topic;
        this.closeAction = closeAction;
    }

    @Override
    public String topic() {
        return topic.name();
    }

    @Override
    public MessiQueuingMessageHandle receive(int timeout, TimeUnit unit) throws InterruptedException, MessiClosedException {
        MessiMessage message = doReceive(timeout, unit);
        if (message == null) {
            return null;
        }
        return new MemoryMessiQueuingMessageHandle(topic, message);
    }

    @Override
    public CompletableFuture<? extends MessiQueuingAsyncMessageHandle> receiveAsync() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                MessiMessage message = doReceive(5, TimeUnit.MINUTES);
                if (message == null) {
                    return null;
                }
                return new MemoryMessiQueuingAsyncMessageHandle(topic, message);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private MessiMessage doReceive(int timeout, TimeUnit unit) throws InterruptedException {
        long expireTimeNano = System.nanoTime() + unit.toNanos(timeout);
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            while (topic.isEmpty()) {
                if (isClosed()) {
                    throw new MessiClosedException();
                }
                long durationNano = expireTimeNano - System.nanoTime();
                if (durationNano <= 0) {
                    return null; // timeout
                }
                topic.awaitProduction(durationNano, TimeUnit.NANOSECONDS);
            }
            MessiMessage message = topic.pollQueue();
            return message;
        } finally {
            topic.unlock();
        }
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

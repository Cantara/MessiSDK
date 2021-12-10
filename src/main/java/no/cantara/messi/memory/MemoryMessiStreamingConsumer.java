package no.cantara.messi.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiNoSuchExternalIdException;
import no.cantara.messi.api.MessiStreamingConsumer;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class MemoryMessiStreamingConsumer implements MessiStreamingConsumer {

    final MemoryMessiShard shard;
    final MemoryMessiTopic topic;
    final AtomicReference<MemoryMessiCursor> position = new AtomicReference<>();
    final Consumer<MemoryMessiStreamingConsumer> closeAction;
    final AtomicBoolean closed = new AtomicBoolean(false);

    MemoryMessiStreamingConsumer(MemoryMessiShard shard, MemoryMessiTopic topic, MemoryMessiCursor initialPosition, Consumer<MemoryMessiStreamingConsumer> closeAction) {
        this.shard = shard;
        Objects.requireNonNull(initialPosition);
        this.topic = topic;
        this.closeAction = closeAction;
        MemoryMessiCursor resolvedCursor = resolve(initialPosition);
        this.position.set(resolvedCursor);
    }

    public MemoryMessiCursor resolve(MemoryMessiCursor unresolvedCursor) {
        switch (unresolvedCursor.type) {
            case AT_ULID:
                return unresolvedCursor;
            case OLDEST_RETAINED:
                return new MemoryMessiCursor.Builder()
                        .ulid(new ULID.Value(0L, 0L))
                        .inclusive(true)
                        .build();
            case NOW:
                return new MemoryMessiCursor.Builder()
                        .ulid(MessiULIDUtils.beginningOf(System.currentTimeMillis()))
                        .inclusive(true)
                        .build();
            case AT_PROVIDER_SEQUENCE:
                return new MemoryMessiCursor.Builder()
                        .ulid(ULID.parseULID(unresolvedCursor.sequenceNumber))
                        .inclusive(true)
                        .build();
            case AT_PROVIDER_TIME:
                return new MemoryMessiCursor.Builder()
                        .ulid(MessiULIDUtils.beginningOf(unresolvedCursor.timestamp.toEpochMilli() + (unresolvedCursor.inclusive ? 0 : 1)))
                        .inclusive(true)
                        .build();
            case AT_EXTERNAL_ID:
                topic.tryLock(5, TimeUnit.SECONDS);
                try {
                    ULID.Value lowerBound = MessiULIDUtils.beginningOf(unresolvedCursor.externalIdTimestamp.toEpochMilli() - unresolvedCursor.externalIdTimestampTolerance.toMillis());
                    ULID.Value upperBound = MessiULIDUtils.beginningOf(unresolvedCursor.externalIdTimestamp.toEpochMilli() + unresolvedCursor.externalIdTimestampTolerance.toMillis());
                    ULID.Value ulid = topic.ulidOf(unresolvedCursor.externalId, lowerBound, upperBound);
                    if (ulid == null) {
                        throw new MessiNoSuchExternalIdException(String.format("externalId not found: %s", unresolvedCursor.externalId));
                    }
                    return new MemoryMessiCursor.Builder()
                            .ulid(ulid)
                            .inclusive(unresolvedCursor.inclusive)
                            .build();
                } finally {
                    topic.unlock();
                }
            default:
                throw new IllegalStateException("Type not implemented: " + unresolvedCursor.type);
        }
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
            position.set(new MemoryMessiCursor.Builder()
                    .ulid(MessiULIDUtils.toUlid(message.getUlid()))
                    .inclusive(false)
                    .build());
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
    public MessiCursor currentPosition() {
        return null;
    }

    @Override
    public void seek(long timestamp) {
        position.set(new MemoryMessiCursor.Builder()
                .ulid(MessiULIDUtils.beginningOf(timestamp))
                .inclusive(true)
                .build());
    }

    @Override
    public MemoryMessiShard shard() {
        return shard;
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

package no.cantara.messi.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiTopic;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;
import no.cantara.messi.protos.MessiProvider;
import no.cantara.messi.protos.MessiUlid;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Optional.ofNullable;

class MemoryMessiTopic implements MessiTopic {

    final AtomicBoolean closed = new AtomicBoolean(false);
    final MemoryMessiClient client;
    final String topic;
    final NavigableMap<ULID.Value, MessiMessage> data = new ConcurrentSkipListMap<>(); // protected by lock
    final ReentrantLock lock = new ReentrantLock();
    final Condition condition = lock.newCondition();
    final ULID ulid = new ULID();
    final AtomicReference<ULID.Value> prevUlid = new AtomicReference<>(ulid.nextValue());

    static class DeliveredMessage {
        final MessiMessage message;
        final Instant deliveredTime;

        DeliveredMessage(MessiMessage message, Instant deliveredTime) {
            this.message = message;
            this.deliveredTime = deliveredTime;
        }
    }

    final BlockingDeque<MessiMessage> primaryQ = new LinkedBlockingDeque<>();
    final Deque<DeliveredMessage> deliveredQ = new ConcurrentLinkedDeque<>();

    final List<MemoryMessiProducer> producers = new CopyOnWriteArrayList<>();
    final AtomicReference<MemoryMessiShard> managedShardRef = new AtomicReference<>();

    MemoryMessiTopic(MemoryMessiClient client, String topic) {
        this.client = client;
        this.topic = topic;
    }

    MessiMessage lastMessage() {
        Map.Entry<ULID.Value, MessiMessage> lastEntry = data.lastEntry();
        if (lastEntry == null) {
            return null;
        }
        return lastEntry.getValue();
    }

    MessiMessage firstMessage() {
        Map.Entry<ULID.Value, MessiMessage> firstEntry = data.firstEntry();
        if (firstEntry == null) {
            return null;
        }
        return firstEntry.getValue();
    }

    MessiMessage pollQueue() {
        checkHasLock();
        Instant now = Instant.now();
        while (true) {
            DeliveredMessage deliveredMessage = deliveredQ.poll();
            if (deliveredMessage == null) {
                break;
            }
            if (deliveredMessage.deliveredTime.plus(30, ChronoUnit.SECONDS).isBefore(now)) {
                // timeout without ACK
                primaryQ.addFirst(deliveredMessage.message);
            } else {
                deliveredQ.addFirst(deliveredMessage);
                break;
            }
        }
        MessiMessage message = primaryQ.poll();
        if (message != null) {
            deliveredQ.addLast(new DeliveredMessage(message, Instant.now()));
        }
        return message;
    }

    boolean ackQueue(MessiMessage message) {
        checkHasLock();
        boolean wasRemoved = deliveredQ.removeIf(deliveredMessage -> MessiULIDUtils.toUlid(deliveredMessage.message.getUlid()).equals(MessiULIDUtils.toUlid(message.getUlid())));
        return wasRemoved;
    }

    private void checkHasLock() {
        if (!lock.isHeldByCurrentThread()) {
            throw new IllegalStateException("The calling thread must hold the lock before calling this method");
        }
    }

    void write(MessiMessage message) {
        checkHasLock();

        ULID.Value ulid;
        if (message.hasUlid()) {
            ulid = new ULID.Value(message.getUlid().getMsb(), message.getUlid().getLsb());
        } else {
            ulid = MessiULIDUtils.nextMonotonicUlid(this.ulid, prevUlid.get());
        }
        prevUlid.set(ulid);

        // fake serialization and deserialization
        MessiMessage.Builder copyBuilder = MessiMessage.newBuilder(message);
        copyBuilder.setUlid(MessiUlid.newBuilder()
                .setMsb(ulid.getMostSignificantBits())
                .setLsb(ulid.getLeastSignificantBits())
                .build());
        long nowMillis = System.currentTimeMillis();
        if (!message.hasFirstProvider()) {
            copyBuilder.setFirstProvider(MessiProvider.newBuilder()
                    .setShardId(firstShard())
                    .setPublishedTimestamp(nowMillis)
                    .setSequenceNumber(ulid.toString())
                    .setTechnology("Messi-inmemory")
                    .build());
        }
        copyBuilder.setProvider(MessiProvider.newBuilder()
                .setShardId(firstShard())
                .setPublishedTimestamp(nowMillis)
                .setSequenceNumber(ulid.toString())
                .setTechnology("Messi-inmemory")
                .build());
        MessiMessage copy = copyBuilder.build();

        data.put(ulid, copy);
        primaryQ.add(copy);

        signalProduction();
    }

    boolean hasNext(MemoryMessiCursor cursor) {
        checkHasLock();
        if (cursor.ulid == null) {
            return !data.isEmpty();
        }
        if (cursor.inclusive) {
            return data.ceilingEntry(cursor.ulid) != null;
        } else {
            return data.higherKey(cursor.ulid) != null;
        }
    }

    MessiMessage readNext(MemoryMessiCursor cursor) {
        checkHasLock();
        if (cursor.ulid == null) {
            return data.firstEntry().getValue();
        }
        if (cursor.inclusive) {
            return ofNullable(data.ceilingEntry(cursor.ulid)).map(Map.Entry::getValue).orElse(null);
        } else {
            return ofNullable(data.higherEntry(cursor.ulid)).map(Map.Entry::getValue).orElse(null);
        }
    }

    boolean tryLock() {
        return lock.tryLock();
    }

    void tryLock(int timeout, TimeUnit unit) {
        try {
            if (!lock.tryLock(timeout, unit)) {
                throw new RuntimeException("timeout while waiting for lock");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    boolean isEmpty() {
        return data.isEmpty();
    }

    void unlock() {
        lock.unlock();
    }

    void awaitProduction(long duration, TimeUnit unit) throws InterruptedException {
        condition.await(duration, unit);
    }

    void signalProduction() {
        condition.signalAll();
    }

    @Override
    public String toString() {
        return "MemoryMessiTopic{" +
                "topic='" + topic + '\'' +
                '}';
    }

    ULID.Value ulidOf(String externalId, ULID.Value lowerBound, ULID.Value upperBound) {
        checkHasLock();

        /*
         * Perform a topic scan looking for the given external-id, starting at lowerBound (inclusive) and ending at upperBound (exclusive)
         */

        for (Map.Entry<ULID.Value, MessiMessage> entry : data.subMap(lowerBound, true, upperBound, false).entrySet()) {
            if (externalId.equals(entry.getValue().getExternalId())) {
                return entry.getKey();
            }
        }

        return null;
    }

    public void delete(MessiMessage messiMessage) {
        checkHasLock();
        ULID.Value messageId = MessiULIDUtils.toUlid(messiMessage.getUlid());
        data.remove(messageId);
    }

    @Override
    public String name() {
        return topic;
    }

    @Override
    public MemoryMessiProducer producer() {
        if (isClosed()) {
            throw new MessiClosedException();
        }
        MemoryMessiProducer producer = new MemoryMessiProducer(this, producers::remove);
        this.producers.add(producer);
        return producer;
    }

    @Override
    public MemoryMessiShard shardOf(String shardId) {
        if (managedShardRef.get() == null) {
            managedShardRef.compareAndSet(null, new MemoryMessiShard(firstShard(), this));
        }
        MemoryMessiShard shard = managedShardRef.get();
        return shard;
    }

    @Override
    public MemoryMessiMetadataClient metadata() {
        return new MemoryMessiMetadataClient(topic);
    }

    @Override
    public MemoryMessiClient client() {
        return client;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        for (MemoryMessiProducer producer : producers) {
            producer.close();
        }
        producers.clear();
        MemoryMessiShard shard = managedShardRef.get();
        if (shard != null) {
            shard.close();
            managedShardRef.set(null);
        }
        data.clear();
        primaryQ.clear();
        deliveredQ.clear();
        closed.set(true);
    }
}

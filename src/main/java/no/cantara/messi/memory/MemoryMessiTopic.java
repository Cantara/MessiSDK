package no.cantara.messi.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.protos.MessiMessage;
import no.cantara.messi.protos.MessiUlid;

import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Optional.ofNullable;

class MemoryMessiTopic {

    final String topic;
    final NavigableMap<ULID.Value, MessiMessage> data = new ConcurrentSkipListMap<>(); // protected by lock
    final ReentrantLock lock = new ReentrantLock();
    final Condition condition = lock.newCondition();

    MemoryMessiTopic(String topic) {
        this.topic = topic;
    }

    MessiMessage lastMessage() {
        checkHasLock();
        if (data.isEmpty()) {
            return null;
        }
        return data.lastEntry().getValue();
    }

    private void checkHasLock() {
        if (!lock.isHeldByCurrentThread()) {
            throw new IllegalStateException("The calling thread must hold the lock before calling this method");
        }
    }

    void write(ULID.Value ulid, MessiMessage message) {
        checkHasLock();
        // fake serialization and deserialization
        MessiMessage copy = MessiMessage.newBuilder(message)
                .setUlid(MessiUlid.newBuilder().setMsb(ulid.getMostSignificantBits()).setLsb(ulid.getLeastSignificantBits()).build())
                .build();
        data.put(ulid, copy);
        signalProduction();
    }

    boolean hasNext(MemoryMessiCursor cursor) {
        checkHasLock();
        if (cursor.startKey == null) {
            return !data.isEmpty();
        }
        if (cursor.inclusive) {
            return data.ceilingEntry(cursor.startKey) != null;
        } else {
            return data.higherKey(cursor.startKey) != null;
        }
    }

    MessiMessage readNext(MemoryMessiCursor cursor) {
        checkHasLock();
        if (cursor.startKey == null) {
            return data.firstEntry().getValue();
        }
        if (cursor.inclusive) {
            return ofNullable(data.ceilingEntry(cursor.startKey)).map(Map.Entry::getValue).orElse(null);
        } else {
            return ofNullable(data.higherEntry(cursor.startKey)).map(Map.Entry::getValue).orElse(null);
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
}

package no.cantara.messi.api;

import de.huxhorn.sulky.ulid.ULID;

import java.time.Duration;
import java.time.Instant;

public interface MessiCursor extends Comparable<MessiCursor> {

    /**
     * Create a checkpoint by serializing this cursor.
     *
     * @return a serialized cursor that can be used as a checkpoint.
     */
    String checkpoint();

    /**
     * Check whether this cursor is before other cursor.
     *
     * @param other the other cursor to check against.
     * @return true if this cursor is pointed to a position in the stream that is before the other cursor.
     * @throws MessiNotCompatibleCursorException if other cursor is not compatible with this cursor, i.e. they cannot be
     *                                           compared to each-other.
     */
    default boolean isBefore(MessiCursor other) throws MessiNotCompatibleCursorException {
        if (!getClass().equals(other.getClass())) {
            throw new MessiNotCompatibleCursorException(String.format("Cursor classes are not compatible. this.getClass(): %s, other.getClass(): %s", getClass(), other.getClass()));
        }
        return compareTo(other) < 0;
    }

    /**
     * Check whether this cursor is after other cursor.
     *
     * @param other the other cursor to check against.
     * @return true if this cursor is pointed to a position in the stream that is after the other cursor.
     * @throws MessiNotCompatibleCursorException if other cursor is not compatible with this cursor, i.e. they cannot be
     *                                           compared to each-other.
     */
    default boolean isAfter(MessiCursor other) throws MessiNotCompatibleCursorException {
        if (!getClass().equals(other.getClass())) {
            throw new MessiNotCompatibleCursorException(String.format("Cursor classes are not compatible. this.getClass(): %s, other.getClass(): %s", getClass(), other.getClass()));
        }
        return compareTo(other) > 0;
    }

    /**
     * Check whether this cursor is the same as other cursor.
     *
     * @param other the other cursor to check against.
     * @return true if this cursor is pointed to the same position in the stream as the other cursor.
     * @throws MessiNotCompatibleCursorException if other cursor is not compatible with this cursor, i.e. they cannot be
     *                                           compared to each-other.
     */
    default boolean isSame(MessiCursor other) throws MessiNotCompatibleCursorException {
        if (!getClass().equals(other.getClass())) {
            throw new MessiNotCompatibleCursorException(String.format("Cursor classes are not compatible. this.getClass(): %s, other.getClass(): %s", getClass(), other.getClass()));
        }
        return compareTo(other) == 0;
    }

    @Override
    int compareTo(MessiCursor o) throws NullPointerException, MessiNotCompatibleCursorException;

    interface Builder {
        Builder shardId(String shardId);

        Builder now();

        Builder oldest();

        Builder providerTimestamp(Instant timestamp);

        Builder providerSequenceNumber(String sequenceNumber);

        Builder ulid(ULID.Value ulid);

        Builder externalId(String externalId, Instant externalIdTimestamp, Duration externalIdTimestampTolerance);

        Builder inclusive(boolean inclusive);

        Builder checkpoint(String checkpoint);

        MessiCursor build();
    }
}

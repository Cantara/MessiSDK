package no.cantara.messi.api;

import de.huxhorn.sulky.ulid.ULID;

import java.time.Duration;
import java.time.Instant;

public interface MessiCursor {

    /**
     * Create a checkpoint by serializing this cursor.
     *
     * @return a serialized cursor that can be used as a checkpoint.
     */
    String checkpoint();

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

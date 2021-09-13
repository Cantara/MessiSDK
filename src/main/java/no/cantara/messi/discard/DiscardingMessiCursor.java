package no.cantara.messi.discard;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiCursor;

import java.time.Duration;
import java.time.Instant;

public class DiscardingMessiCursor implements MessiCursor {

    static class Builder implements MessiCursor.Builder {
        @Override
        public Builder shardId(String shardId) {
            return this;
        }

        @Override
        public MessiCursor.Builder now() {
            return this;
        }

        @Override
        public MessiCursor.Builder oldest() {
            return this;
        }

        @Override
        public Builder providerTimestamp(Instant timestamp) {
            return this;
        }

        @Override
        public Builder providerSequenceNumber(String sequenceNumber) {
            return this;
        }

        @Override
        public Builder ulid(ULID.Value ulid) {
            return this;
        }

        @Override
        public MessiCursor.Builder externalId(String externalId, Instant externalIdTimestamp, Duration externalIdTimestampTolerance) {
            return this;
        }

        @Override
        public Builder inclusive(boolean inclusive) {
            return this;
        }

        @Override
        public DiscardingMessiCursor build() {
            return new DiscardingMessiCursor();
        }
    }
}

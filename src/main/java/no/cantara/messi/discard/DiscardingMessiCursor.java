package no.cantara.messi.discard;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiNotCompatibleCursorException;

import java.time.Duration;
import java.time.Instant;

public class DiscardingMessiCursor implements MessiCursor {

    @Override
    public String checkpoint() {
        return "";
    }

    @Override
    public int compareTo(MessiCursor o) throws NullPointerException, MessiNotCompatibleCursorException {
        throw new MessiNotCompatibleCursorException("This cursor can never be compared to anything, please use a different provider");
    }

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
        public MessiCursor.Builder checkpoint(String checkpoint) {
            return this;
        }

        @Override
        public DiscardingMessiCursor build() {
            return new DiscardingMessiCursor();
        }
    }
}

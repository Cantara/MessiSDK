package no.cantara.messi.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiCursor;

import java.util.Objects;

public class MemoryMessiCursor implements MessiCursor {

    /**
     * Need not exactly match an existing ulid-value.
     */
    final ULID.Value startKey;

    /**
     * Whether or not to include the element with ulid-value matching the lower-bound exactly.
     */
    final boolean inclusive;

    /**
     * Traversal direction, true signifies forward.
     */
    final boolean forward;

    MemoryMessiCursor(ULID.Value startKey, boolean inclusive, boolean forward) {
        this.startKey = startKey;
        this.inclusive = inclusive;
        this.forward = forward;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemoryMessiCursor that = (MemoryMessiCursor) o;
        return inclusive == that.inclusive &&
                forward == that.forward &&
                startKey.equals(that.startKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startKey, inclusive, forward);
    }

    @Override
    public String toString() {
        return "MemoryCursor{" +
                "startKey=" + startKey +
                ", inclusive=" + inclusive +
                ", forward=" + forward +
                '}';
    }
}

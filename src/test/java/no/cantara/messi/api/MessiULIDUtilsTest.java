package no.cantara.messi.api;

import de.huxhorn.sulky.ulid.ULID;
import org.testng.annotations.Test;

public class MessiULIDUtilsTest {

    @Test
    public void nextMonotonicUlidDoesNotThrowExceptionWhenClockMovesForward() {
        ULID ulid = new ULID();
        ULID.Value prev = ulid.nextValue(System.currentTimeMillis() - 5000);
        ULID.Value value = MessiULIDUtils.nextMonotonicUlid(ulid, prev);
    }

    @Test
    public void nextMonotonicUlidDoesNotThrowExceptionWhenClockMovesBackwardBySmallAmount() {
        ULID ulid = new ULID();
        ULID.Value prev = ulid.nextValue(System.currentTimeMillis() + 1000);
        ULID.Value value = MessiULIDUtils.nextMonotonicUlid(ulid, prev);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void nextMonotonicUlidThrowsExceptionWhenClockMovesBackwardByLargeAmount() {
        ULID ulid = new ULID();
        ULID.Value prev = ulid.nextValue(System.currentTimeMillis() + 50000);
        ULID.Value value = MessiULIDUtils.nextMonotonicUlid(ulid, prev);
    }
}

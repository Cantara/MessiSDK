package no.cantara.messi.api;

import com.google.protobuf.Timestamp;
import org.testng.annotations.Test;

import java.time.Instant;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class MessiTimestampUtilsTest {

    @Test
    public void thatMillisZeroWorks() {
        Timestamp timestamp = MessiTimestampUtils.toTimestamp(0); // "1970-01-01T00:00:00Z"
        assertEquals(timestamp.getSeconds(), 0);
        assertEquals(timestamp.getNanos(), 0);
    }

    @Test
    public void thatMillisOneWorks() {
        Timestamp timestamp = MessiTimestampUtils.toTimestamp(1); // "1970-01-01T00:00:01Z"
        assertEquals(timestamp.getSeconds(), 0);
        assertEquals(timestamp.getNanos(), 1000000);
    }

    @Test
    public void thatMillisThousandWorks() {
        Timestamp timestamp = MessiTimestampUtils.toTimestamp(1000); // "1970-01-01T00:00:01Z"
        assertEquals(timestamp.getSeconds(), 1);
        assertEquals(timestamp.getNanos(), 0);
    }

    @Test
    public void thatSpecificMillisWorks() {
        Timestamp timestamp = MessiTimestampUtils.toTimestamp(1638954370038L); // "2021-12-08T09:06:10.038Z"
        assertEquals(timestamp.getSeconds(), 1638954370L);
        assertEquals(timestamp.getNanos(), 38000000);
    }

    @Test
    public void thatSpecificInstantWorks() {
        Timestamp timestamp = MessiTimestampUtils.toTimestamp(Instant.parse("2021-12-08T09:06:10.038Z"));
        assertEquals(timestamp.getSeconds(), 1638954370L);
        assertEquals(timestamp.getNanos(), 38000000);
    }

    @Test
    public void thatSpecificTimestampWorks() {
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(1638954370L)
                .setNanos(38000000)
                .build();
        Instant instant = MessiTimestampUtils.toInstant(timestamp);
        assertEquals(instant.toString(), "2021-12-08T09:06:10.038Z");
    }

    @Test
    public void thatNowWorks() {
        Instant nowInstant = Instant.now();
        Timestamp nowTimestamp = MessiTimestampUtils.now();
        assertTrue(Math.abs(nowInstant.getEpochSecond() - nowTimestamp.getSeconds()) < 3);
    }

    @Test
    public void thatNegativeSecondsWorks() {
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(-12345678901L)
                .setNanos(4200000)
                .build();
        Instant instant = MessiTimestampUtils.toInstant(timestamp);
        assertEquals(instant.toString(), "1578-10-13T04:44:59.004200Z");
    }

    @Test
    public void thatInstantBeforeEpochWorks() {
        Instant instant = Instant.parse("1578-10-13T04:44:59.004200Z");
        Timestamp timestamp = MessiTimestampUtils.toTimestamp(instant);
        assertEquals(timestamp.getSeconds(), -12345678901L);
        assertEquals(timestamp.getNanos(), 4200000);
    }
}

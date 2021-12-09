package no.cantara.messi.api;

import com.google.protobuf.Timestamp;

import java.time.Instant;

public class MessiTimestampUtils {

    public static Timestamp now() {
        return toTimestamp(Instant.now());
    }

    public static Timestamp toTimestamp(long epochMillis) {
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(epochMillis / 1000)
                .setNanos((int) ((epochMillis % 1000) * 1000000))
                .build();
        return timestamp;
    }

    public static Timestamp toTimestamp(Instant instant) {
        long seconds = instant.getEpochSecond();
        int nanos = instant.getNano();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(seconds)
                .setNanos(nanos)
                .build();
        return timestamp;
    }

    public static Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}

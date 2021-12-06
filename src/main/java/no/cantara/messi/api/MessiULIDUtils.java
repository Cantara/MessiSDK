package no.cantara.messi.api;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.protos.MessiUlid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessiULIDUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MessiULIDUtils.class);

    public static ULID.Value toUlid(MessiUlid messiUlid) {
        return new ULID.Value(messiUlid.getMsb(), messiUlid.getLsb());
    }

    public static MessiUlid toMessiUlid(ULID.Value ulid) {
        return MessiUlid.newBuilder()
                .setMsb(ulid.getMostSignificantBits())
                .setLsb(ulid.getLeastSignificantBits())
                .build();
    }

    /**
     * Return the ULID.Value that represents beginning of a given timestamp. Be careful not to use these as "random"
     * IDs, these are intended for searching.
     *
     * @param timestamp the timestamp component of the returned ulid.
     * @return the beginning-of-time ulid
     */
    public static ULID.Value beginningOf(long timestamp) {
        return new ULID.Value((timestamp << 16) & 0xFFFFFFFFFFFF0000L, 0L);
    }

    /**
     * Return the ULID.Value that represents beginning of all time. Be careful not to use these as "random"
     * IDs, these are intended for searching.
     *
     * @return the beginning-of-time ulid
     */
    public static ULID.Value beginningOfTime() {
        return new ULID.Value(0, 0);
    }

    /**
     * Generate a new unique ulid. If the previous timestamp is in the future, this method will block (sleep) the
     * current thread (up to 30 seconds) until that time is reached in order to avoid non-increasing IDs. Newly
     * generated IDs will be using the time of the system-clock and a random part for the ID unless previous id has same
     * timestamp, then it will use the previous id + 1.
     *
     * @param generator    the ulid generator
     * @param previousUlid the previous ulid in the sequence
     * @return the generated ulid
     */
    public static ULID.Value nextMonotonicUlid(ULID generator, ULID.Value previousUlid) {
        /*
         * Will spin until time ticks if next value overflows.
         * Although theoretically possible, it is extremely unlikely that the loop will ever spin
         */
        ULID.Value value;
        do {
            long timestamp = System.currentTimeMillis();
            long diff = timestamp - previousUlid.timestamp();
            if (diff < 0) {
                if (diff < -(30 * 1000)) {
                    throw new IllegalStateException(String.format("Previous timestamp is in the future. Diff %d ms", -diff));
                }
                LOG.debug("Previous timestamp is in the future, waiting for time to catch up. Diff {} ms", -diff);
                try {
                    Thread.sleep(-diff);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            // diff >= 0
            value = generator.nextStrictlyMonotonicValue(previousUlid, timestamp).orElse(null);
        } while (value == null);
        return value;
    }
}

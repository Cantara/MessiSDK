package no.cantara.messi.api;

import de.huxhorn.sulky.ulid.ULID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessiULIDUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MessiULIDUtils.class);

    /**
     * Return the ULID.Value that represents beginning of a given timestamp.
     *
     * @param timestamp the timestamp component of the returned ulid.
     * @return the beginning-of-time ulid
     */
    public static ULID.Value beginningOf(long timestamp) {
        return new ULID.Value((timestamp << 16) & 0xFFFFFFFFFFFF0000L, 0L);
    }

    /**
     * Return the ULID.Value that represents beginning of all time.
     *
     * @return the beginning-of-time ulid
     */
    public static ULID.Value beginningOfTime() {
        return new ULID.Value(0, 0);
    }

    /**
     * Generate a new unique ulid. If the newly generated ulid has a new timestamp than the previous one, then the very
     * least significant bit will be set to 1 (which is higher than beginning-of-time ulid used by consumer).
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
            } else if (diff > 0) {
                // start at lsb 1, to avoid inclusive/exclusive semantics when searching
                return new ULID.Value((timestamp << 16) & 0xFFFFFFFFFFFF0000L, 1L);
            }
            // diff == 0
            value = generator.nextStrictlyMonotonicValue(previousUlid, timestamp).orElse(null);
        } while (value == null);
        return value;
    }
}

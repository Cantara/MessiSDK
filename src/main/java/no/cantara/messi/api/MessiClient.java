package no.cantara.messi.api;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.protos.MessiMessage;

import java.time.Duration;

public interface MessiClient extends AutoCloseable {

    /**
     * Create a new producer on the given topic. The producer can be used to produce messages on the topic stream.
     *
     * @param topic the name of the topic to produce messages on. Must be the context-specific short-name of the topic
     *              that is independent of any technology or implementation specific schemes which should be configured
     *              when loading the client provider.
     * @return
     */
    MessiProducer producer(String topic);

    /**
     * Create a new consumer on the given topic, starting at the very beginning of the topic.
     *
     * @param topic the name of the topic to consume message from. Must be the context-specific short-name of the topic
     *              that is independent of any technology or implementation specific schemes which should be configured
     *              when loading the client provider.
     * @return a consumer that can be used to read the topic stream.
     */
    default MessiConsumer consumer(String topic) {
        return consumer(topic, (MessiCursor) null);
    }

    /**
     * Create a new consumer on the given topic, starting at the given initial position (or at the beginning of the topic
     * if the given initial position is null).
     *
     * @param topic  the name of the topic to consume message from. Must be the context-specific short-name of
     *               the topic that is independent of any technology or implementation specific schemes which
     *               should be configured when loading the client provider.
     * @param cursor the cursor to use when messages are read from the topic.
     * @return a consumer that can be used to read the topic stream.
     */
    MessiConsumer consumer(String topic, MessiCursor cursor);

    /**
     * Create a new consumer on the given topic, starting at the given initial position (or at the beginning of the topic
     * if the given initial position is null). The consumer will not include the initial-position itself when reading
     * the stream.
     *
     * @param topic           the name of the topic to consume message from. Must be the context-specific short-name of
     *                        the topic that is independent of any technology or implementation specific schemes which
     *                        should be configured when loading the client provider.
     * @param initialPosition the position to be set as current position when creating the consumer
     * @return a consumer that can be used to read the topic stream.
     */
    default MessiConsumer consumer(String topic, ULID.Value initialPosition) {
        return consumer(topic, initialPosition == null ? null : cursorOf(topic, initialPosition, false));
    }

    /**
     * Create a new consumer on the given topic, starting at the given initial position (or at the beginning of the topic
     * if the given initial position is null).
     *
     * @param topic           the name of the topic to consume message from. Must be the context-specific short-name of
     *                        the topic that is independent of any technology or implementation specific schemes which
     *                        should be configured when loading the client provider.
     * @param initialPosition the position to be set as current position when creating the consumer
     * @param inclusive       whether or not to include the message at the initial-position when reading the stream
     * @return a consumer that can be used to read the topic stream.
     */
    default MessiConsumer consumer(String topic, ULID.Value initialPosition, boolean inclusive) {
        return consumer(topic, initialPosition == null ? null : cursorOf(topic, initialPosition, inclusive));
    }

    /**
     * Create a new consumer on the given topic, starting at the given initial position (or at the beginning of the topic
     * if the given initial position is null). The consumer will not include the initial-position itself when reading
     * the stream.
     *
     * @param topic             the name of the topic to consume message from. Must be the context-specific short-name of
     *                          the topic that is independent of any technology or implementation specific schemes which
     *                          should be configured when loading the client provider.
     * @param initialExternalId the externalId to be set as current position when creating the consumer
     * @param approxTimestamp   timestamp in milliseconds since 1/1-1970. This is an approximation of when the
     *                          initialExternalId was originally written to the stream.
     * @param tolerance         tolerance duration. ExternalId can be scanned within the range [approxTimestamp - tolerance,
     *                          approxTimestamp + tolerance)
     * @return a consumer that can be used to read the topic stream.
     */
    default MessiConsumer consumer(String topic, String initialExternalId, long approxTimestamp, Duration tolerance) {
        return consumer(topic, initialExternalId == null ? null : cursorOf(topic, initialExternalId, false, approxTimestamp, tolerance));
    }

    /**
     * Create a new consumer on the given topic, starting at the given initial position (or at the beginning of the topic
     * if the given initial position is null).
     *
     * @param topic             the name of the topic to consume message from. Must be the context-specific short-name of
     *                          the topic that is independent of any technology or implementation specific schemes which
     *                          should be configured when loading the client provider.
     * @param initialExternalId the position to be set as current position when creating the consumer
     * @param inclusive         whether or not to include the message at the initial-position when reading the stream
     * @param approxTimestamp   timestamp in milliseconds since 1/1-1970. This is an approximation of when the
     *                          initialExternalId was originally written to the stream.
     * @param tolerance         tolerance duration. Position can be scanned within the range [approxTimestamp - tolerance,
     *                          approxTimestamp + tolerance)
     * @return a consumer that can be used to read the topic stream.
     */
    default MessiConsumer consumer(String topic, String initialExternalId, boolean inclusive, long approxTimestamp, Duration tolerance) {
        return consumer(topic, initialExternalId == null ? null : cursorOf(topic, initialExternalId, inclusive, approxTimestamp, tolerance));
    }

    /**
     * Find the first message that matches the given ulid in the topic, and return a cursor representing that.
     *
     * @param topic     the topic
     * @param ulid      the ulid representing the starting point in the stream
     * @param inclusive whether the starting point should be included when iterating from the returned cursor
     * @return a cursor starting at the given ulid if a match is found, null otherwise
     */
    MessiCursor cursorOf(String topic, ULID.Value ulid, boolean inclusive);

    /**
     * Find the first message that matches the given position in the topic, and return a cursor representing that. The
     * position will be searched within the range as defined by approxTimestamp and tolerance.
     *
     * @param topic           the topic
     * @param externalId      the position to find
     * @param inclusive       whether the starting point should be included when iterating from the returned cursor
     * @param approxTimestamp timestamp in milliseconds since 1/1-1970. This is an approximation of when the
     *                        initialPosition was originally written to the stream.
     * @param tolerance       tolerance duration. Position can be scanned within the range [approxTimestamp - tolerance,
     *                        approxTimestamp + tolerance)
     * @return a cursor starting at the given position if a match is found, null otherwise
     * @throws MessiNoSuchExternalIdException if the position cannot be found within the bounds of the approcTimestamp +- tolerance
     */
    MessiCursor cursorOf(String topic, String externalId, boolean inclusive, long approxTimestamp, Duration tolerance) throws MessiNoSuchExternalIdException;

    /**
     * Will read and return the last message in the stream.
     *
     * @param topic the name of the topic to read the last message position from.
     * @return the current last message in the stream
     * @throws MessiClosedException if the producer was closed before or is closed during this call.
     */
    MessiMessage lastMessage(String topic) throws MessiClosedException;

    /**
     * Returns whether or not the client is closed.
     *
     * @return whether the client is closed.
     */
    boolean isClosed();

    @Override
    void close();

    /**
     * Return a metadata-client that can be used for reading and writing metadata for the given topic.
     *
     * @param topic the topic that the metadata-client will be associated with.
     * @return a metadata-client for the given topic.
     */
    MessiMetadataClient metadata(String topic);
}

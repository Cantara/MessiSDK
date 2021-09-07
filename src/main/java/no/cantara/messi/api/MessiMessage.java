package no.cantara.messi.api;

import de.huxhorn.sulky.ulid.ULID;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface MessiMessage {

    static MessiMessage.Builder builder() {
        return new MessiDefaultMessage.Builder();
    }

    MessiMessage.Builder copy();

    /**
     * A unique lexicographically sortable identifier for the message. Sorting by this id, will also sort the messages
     * by time, making it convenient
     *
     * @return
     */
    ULID.Value ulid();

    /**
     * @return the id of the source of the message as defined by the client.
     */
    String clientSourceId();

    /**
     * Timestamp represented as milliseconds since 1/1-1970. This is derived from the ulid.
     *
     * @return
     */
    default long clientPublishedTimestamp() {
        return ulid().timestamp();
    }

    /**
     * Timestamp represented as milliseconds since 1/1-1970. This is the timestamp set by the underlying provider when
     * the message was received.
     *
     * @return
     */
    long providerPublishedTimestamp();

    /**
     * @return the shard-id or partition-id used by the provider, or null if the provider does not operate with shard
     * or partition.
     */
    String providerShardId();

    /**
     * Sequence number as set by the underlying provider. The sequence number is a unique id of the message within the
     * shard/partition of the underlying topic.
     *
     * @return
     */
    String providerSequenceNumber();

    /**
     * Get the partition-key used to group data by shard in the underlying topic.
     *
     * @return
     */
    String partitionKey();

    /**
     * A group used to maintain which messages belong to the same sequence. The sequence of messages within the same
     * ordering-group is guaranteed. If there are multiple producers using the same ordering-group on the same topic,
     * then either the underlying technology must maintain order (in which the sequenceNumber can be 0), or the
     * producers must synchronize externally to ensure that the sequence-numbers are strictly increasing by 1 for each
     * next message in the stream belonging to the same ordering-group.
     *
     * @return the ordering group.
     */
    String orderingGroup();

    /**
     * A sequence-number (greater-than 0) within the stream, or 0 if the stream already guarantees ordering.
     * <p>
     * These sequence numbers are based on a counter that starts at 1 and increases by 1 for each message in the ordered
     * stream. Because there will be no gaps in this sequence, it's possible for the consumer to re-order the messages
     * in a timely manner when using streaming technology where ordering is not guaranteed, but a best-effort thing.
     *
     * @return the sequence number
     */
    long sequenceNumber();

    /**
     * The original external identifier for the streaming element. Typically the one used by the stream source system.
     * This identifier does not need to be sortable.
     *
     * @return the external-id
     */
    String externalId();

    /**
     * A set of content-keys for this message. Use the get() method to get the corresponding content-value.
     *
     * @return the content-keys
     */
    Set<String> keys();

    /**
     * Get the content for a given key.
     *
     * @param key the content-key
     * @return the raw bytes of the content
     */
    byte[] get(String key);

    /**
     * Get the set of all attributes in the message.
     *
     * @return the set of attributes set on this message
     */
    Set<String> attributes();

    /**
     * Get the map of all attributes in the message.
     *
     * @return the map of attributes on this message
     */
    Map<String, String> attributeMap();

    /**
     * Get the value of an attribute by key.
     *
     * @param key the attribute-key
     * @return the attribute-value
     */
    String attribute(String key);

    /**
     * Convenience method to get a map of all the content key value mappings.
     *
     * @return a map of the data.
     */
    default Map<String, byte[]> data() {
        return keys().stream().collect(Collectors.toMap(Function.identity(), this::get, (a, b) -> a, LinkedHashMap::new));
    }

    /**
     * Builder used to build messages.
     */
    interface Builder {

        /**
         * Set the source-id as defined by the client.
         *
         * @param clientSourceId
         * @return
         */
        Builder clientSourceId(String clientSourceId);

        /**
         * The shardId as set by the provider.
         *
         * @param providerShardId
         * @return
         */
        Builder providerShardId(String providerShardId);

        /**
         * The sequence within the shard as set by the provider. When the underlying provider operates with just a
         * single shard, i.e. has no concept of shards/partitions, this field will also correlate with message-id.
         *
         * @param providerSequenceNumber
         * @return
         */
        Builder providerSequenceNumber(String providerSequenceNumber);

        /**
         * Set the published-provider-timestamp as defined by the underlying provider.
         *
         * @return this builder
         */
        Builder providerPublishedTimestamp(long providerPublishedTimestamp);

        /**
         * Set the partition-key used to group data by shard in the underlying topic.
         *
         * @return this builder
         */
        Builder partitionKey(String partitionKey);

        /**
         * Assign an ordering-group to this message. Messages within the same ordering-group (those that provide the same
         * value for the group parameter) will be re-ordered by consumers if necessary to guarantee fifo ordering within
         * each group.
         *
         * @param orderingGroup the name of the group, must be unique
         * @return this builder
         */
        Builder orderingGroup(String orderingGroup);

        /**
         * Assign the sequence-number > 0. 0 will be used if none is assigned.
         *
         * @param sequenceNumber
         * @return this builder
         * @throws IllegalArgumentException if the sequenceNumber is less than 1.
         */
        Builder sequenceNumber(long sequenceNumber);

        /**
         * Set ulid value of this message. If not supplied, one will be generated by the publisher at publication time.
         * If the sequence-number is not used (value of 0), the ulid value must be strictly monotonically increasing
         * from one message to the next message in a stream in order to preserve the correct order of messages. If the
         * sequence-number is used, then the ulid value sequencing can be done in a best-effort manner.
         *
         * @param ulid
         * @return this builder
         */
        Builder ulid(ULID.Value ulid);

        /**
         * The external identifier of the element in the stream.
         *
         * @param externalId
         * @return this builder
         */
        Builder externalId(String externalId);

        /**
         * Get the value of an attribute by key.
         *
         * @param key the attribute-key
         * @return the attribute-value
         */
        Builder attribute(String key, String value);

        /**
         * Assign binary content to a named key.
         *
         * @param key     the named key
         * @param payload the content
         * @return this builder
         */
        Builder put(String key, byte[] payload);

        /**
         * Put all values from provided map into internal builder map.
         *
         * @param data the data
         * @return this builder
         */
        default Builder data(Map<String, byte[]> data) {
            data.forEach(this::put);
            return this;
        }

        /**
         * Build a message by the properties defined through previous calls to this builder. If some requirements are
         * missing, this call will throw a runtime-exception.
         *
         * @return the message
         */
        MessiMessage build();
    }
}

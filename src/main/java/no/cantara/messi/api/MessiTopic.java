package no.cantara.messi.api;

import java.util.List;

public interface MessiTopic extends AutoCloseable {

    /**
     * @return the name of the topic from which this consumer will consume messages from.
     */
    String name();

    /**
     * Create a new producer on the topic. The producer can be used to produce messages on the topic stream.
     *
     * @return the producer.
     */
    MessiProducer producer();

    /**
     * List all shards in topic. A null value indicates that the underlying technology does not support shards or does
     * not support client control of shards. An empty list indicates that shards are supported by the underlying
     * technology, but that the topic either does not exist or does not have any shards yet. A null value returned from
     * this method indicates that null should be passed to other methods that require shardId, or use one of the default
     * methods that does not require shardId to be specified.
     *
     * @return a list of names of the shards that exists in the topic, or null if shards are not supported.
     */
    default List<String> shards() {
        return null;
    }

    /**
     * The first shard in the list. A convenience method for use-cases where either providers do not support shards or
     * partitions (or does not allow client control of shard), or when the provider supports it but the system using it
     * only ever uses a single shard. The return of this method can always be used to get a {@link MessiShard}.
     *
     * @return the first shard if multiple shards are supported, or a fixed string if shards are not supported.
     */
    default String firstShard() {
        return "the-only-shard";
    }

    MessiShard shardOf(String shardId);

    /**
     * Return a metadata-client that can be used for reading and writing metadata for this topic.
     *
     * @return a metadata-client for this topic.
     */
    MessiMetadataClient metadata();

    boolean isClosed();

    @Override
    void close();
}

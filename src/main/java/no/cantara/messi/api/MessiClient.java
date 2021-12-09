package no.cantara.messi.api;

import no.cantara.messi.protos.MessiMessage;

import java.util.List;

public interface MessiClient extends AutoCloseable {

    /**
     * Returns a topic with the given name. Topics can be used to create producers and queuing/streaming consumers.
     * Creates the topic if necessary. The returned {@link MessiTopic} is a client-side representation of a real topic,
     * it is not necessarily the topic itself.
     *
     * @param name the name of the topic to get or create
     * @return client-side representation of the topic with the given name
     */
    MessiTopic topicOf(String name);

    /**
     * Create a new producer on the given topic. The producer can be used to produce messages on the topic stream.
     *
     * @param topicName the name of the topic to produce messages on. Must be the context-specific short-name of the topic
     *                  that is independent of any technology or implementation specific schemes which should be configured
     *                  when loading the client provider.
     * @return
     * @deprecated Use {@link #topicOf(String)} instead.
     */
    @Deprecated
    default MessiProducer producer(String topicName) {
        MessiTopic topic = topicOf(topicName);
        return topic.producer();
    }

    /**
     * Create a new consumer on the given topic, starting at the very beginning of the topic.
     *
     * @param topicName the name of the topic to consume message from. Must be the context-specific short-name of the topic
     *                  that is independent of any technology or implementation specific schemes which should be configured
     *                  when loading the client provider.
     * @return a consumer that can be used to read the topic stream.
     * @deprecated Use {@link #topicOf(String)} instead.
     */
    @Deprecated
    default MessiConsumer consumer(String topicName) {
        return consumer(topicName, null);
    }

    /**
     * Create a new consumer on the given topic, starting at the given initial position (or at the beginning of the topic
     * if the given initial position is null).
     *
     * @param topicName       the name of the topic to consume message from. Must be the context-specific short-name of
     *                        the topic that is independent of any technology or implementation specific schemes which
     *                        should be configured when loading the client provider.
     * @param initialPosition the cursor to use when messages are read from the topic.
     * @return a consumer that can be used to read the topic stream.
     * @deprecated Use {@link #topicOf(String)} instead.
     */
    @Deprecated
    default MessiConsumer consumer(String topicName, MessiCursor initialPosition) {
        MessiTopic topic = topicOf(topicName);
        String theShardId = topic.firstShard();
        MessiShard shard = topic.shardOf(theShardId);
        if (shard.supportsStreaming()) {
            MessiCursor cursor = initialPosition;
            if (cursor == null) {
                cursor = shard.cursorAtTimeHorizon(); // the oldest retained message
            }
            MessiStreamingConsumer streamingConsumer = shard.streamingConsumer(cursor);
            return new SimplifiedStreamingConsumer(shard, streamingConsumer);
        } else if (shard.supportsQueuing()) {
            MessiQueuingConsumer queuingConsumer = shard.queuingConsumer();
            return new SimplifiedQueuingConsumer(shard, queuingConsumer);
        } else {
            throw new UnsupportedOperationException("Provider does not support queuing or streaming consumers.");
        }
    }

    /**
     * Create a new cursor builder. The builder can be used to tune where a consumer should be able to start reading
     * messages from.
     *
     * @return the builder.
     * @deprecated Use {@link #topicOf(String)} instead.
     */
    @Deprecated
    MessiCursor.Builder cursorOf();

    /**
     * List all shards in topic. A null value indicates that the underlying technology does not support shards or does
     * not support client control of shards. An empty list indicates that shards are supported by the underluying
     * technology, but that the topic either does not exist or does not have any data yet. A null value returned from
     * this method indicates that null should be passed to other methods that require shardId, or use one of the default
     * methods that does not require shardId to be specified.
     *
     * @param topic the topic to list the shards for.
     * @return a list of names of the shards that exists in the topic, or null if shards are not supported.
     * @deprecated Use {@link #topicOf(String)} instead.
     */
    @Deprecated
    default List<String> shards(String topic) {
        return null;
    }

    /**
     * Will read and return the last message in the stream. If the underlying technology does not support this kind of
     * operation without potentially scanning all messages in the stream, this method may throw an
     * {@link java.lang.UnsupportedOperationException}
     *
     * @param topic the name of the topic to read the last message position from.
     * @return the current last message in the stream
     * @throws MessiClosedException          if the producer was closed before or is closed during this call.
     * @throws UnsupportedOperationException if the underlying technology does not support efficiently reading the last
     *                                       message in the topic.
     * @deprecated Use {@link #topicOf(String)} instead.
     */
    @Deprecated
    default MessiMessage lastMessage(String topic) throws MessiClosedException {
        return lastMessage(topic, null);
    }

    /**
     * Will read and return the last message in the stream. If the underlying technology does not support this kind of
     * operation without potentially scanning all messages in the stream, this method may throw an
     * {@link java.lang.UnsupportedOperationException}
     *
     * @param topic   the name of the topic to read the last message position from.
     * @param shardId the id of the shard/partition to get the last-message from.
     * @return the current last message in the stream
     * @throws MessiClosedException          if the producer was closed before or is closed during this call.
     * @throws UnsupportedOperationException if the underlying technology does not support efficiently reading the last
     *                                       message in the topic.
     * @deprecated Use {@link #topicOf(String)} instead.
     */
    @Deprecated
    MessiMessage lastMessage(String topic, String shardId) throws MessiClosedException;

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
     * @deprecated Use {@link #topicOf(String)} instead.
     */
    @Deprecated
    MessiMetadataClient metadata(String topic);
}

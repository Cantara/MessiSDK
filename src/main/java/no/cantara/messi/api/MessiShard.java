package no.cantara.messi.api;

import no.cantara.messi.protos.MessiMessage;

public interface MessiShard extends AutoCloseable {

    /**
     * Whether this consumer is a queuing-based consumer. A queuing-based consumer will always have a stateful
     * subscription counterpart on the server-side that keeps track of the progress of the client in the messaging
     * queue. Clients need to delete the message on the server after they are done processing a message in order to
     * ensure that the message is not delivered again. The queuing-based consumer guarantees at-least-once semantics,
     * and this is the easiest of the two consumer models to use as the client doesn't need to keep track of its own
     * overall progress. The queuing-based consumer model might produce spurious duplicates on the server-side, and
     * will re-deliver messages, possibly out-of-oder, in the case where clients have not deleted messages in a timely
     * manner. The queueing-based consumer will however do a best-effort to deliver messages one time each and in
     * the same order in which they were produced.
     *
     * @return
     */
    default boolean supportsQueuing() {
        return false;
    }

    /**
     * If the {@link #supportsQueuing()} method returns true, then this method will return a
     * {@link MessiQueuingConsumer}, otherwise an {@link UnsupportedOperationException} is thrown.
     *
     * @return
     */
    default MessiQueuingConsumer queuingConsumer() {
        throw new UnsupportedOperationException();
    }

    /**
     * Whether this consumer is a streaming-based consumer. A streaming-based consumer relies on the client to keep
     * track of the position in the stream. Typically, clients will need to checkpoint their progress in the stream
     * regularly in case of client failure. The streaming-based consumer guarantees at-least-once semantics, and this
     * is the most scalable of the two consumer models as no messages need to be deleted. The steaming-based consumer
     * will never produce out-of-order messages, the only way to see a message more than once is if the producer
     * produced it more than once, or if the client asks for it more than once, typically something that can happen
     * after recovery from checkpoint.
     *
     * @return
     */
    default boolean supportsStreaming() {
        return false;
    }

    /**
     * If the {@link #supportsStreaming()} ()} method returns true, then this method will return a
     * {@link MessiStreamingConsumer}, otherwise an {@link UnsupportedOperationException} is thrown.
     *
     * @param initialPosition the initial position in the stream.
     * @return a streaming consumer
     */
    default MessiStreamingConsumer streamingConsumer(MessiCursor initialPosition) {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a new cursor builder. The builder can be used to tune where a consumer should be able to start reading
     * messages from.
     *
     * @return the builder.
     */
    MessiCursor.Builder cursorOf();

    /**
     * Create a new cursor from checkpoint.
     *
     * @return the cursor.
     */
    MessiCursor cursorOfCheckpoint(String checkpoint);

    /**
     * Creates a cursor that points right before the provided message. This can typically be used for checkpointing.
     *
     * @param message
     * @return
     * @throws IllegalArgumentException if the message did not come from this stream or is otherwise not compatible
     */
    MessiCursor cursorAt(MessiMessage message);

    /**
     * Creates a cursor that points right after the provided message. This can typically be used for checkpointing.
     *
     * @param message
     * @return
     * @throws IllegalArgumentException if the message did not come from this stream or is otherwise not compatible
     */
    MessiCursor cursorAfter(MessiMessage message);

    /**
     * Will return a cursor pointed at the last message in the stream, so that the next message returned will be the
     * current last message in the stream. If the underlying technology does not support this kind of operation without
     * potentially scanning all messages in the stream, this method may throw an
     * {@link java.lang.UnsupportedOperationException} The last message in the stream is computed when this method is
     * called, there is no guarantee that the same message will be the last message after returning from this method if
     * more messages are produced.
     *
     * @return the current last message in the stream
     * @throws MessiClosedException          if the producer was closed before or is closed during this call.
     * @throws UnsupportedOperationException if the underlying technology does not support efficiently reading the last
     *                                       message in the topic.
     */
    MessiCursor cursorAtLastMessage() throws MessiClosedException;

    /**
     * Will return a cursor pointed after the last message in the stream, so that the next message returned will be the
     * message that comes after the current last message in the stream. If the underlying technology does not support
     * this kind of operation without potentially scanning all messages in the stream, this method may throw an
     * {@link java.lang.UnsupportedOperationException}. The last message in the stream is computed when this method is
     * called, there is no guarantee that the same message will be the last message after returning from this method if
     * more messages are produced.
     *
     * @return the current last message in the stream
     * @throws MessiClosedException          if the producer was closed before or is closed during this call.
     * @throws UnsupportedOperationException if the underlying technology does not support efficiently reading the last
     *                                       message in the topic.
     */
    MessiCursor cursorAfterLastMessage() throws MessiClosedException;

    /**
     * Will return a cursor pointed after the last message in the stream, so that whenever this cursor is used, it will
     * always point to the next message that will be produced. This is different from {@link #cursorAfterLastMessage()}
     * in that it does not fix the cursor position when calling this method, but the next message to read is computed at
     * the time the cursor is being used, not when it's created.
     *
     * @return the current last message in the stream
     * @throws MessiClosedException          if the producer was closed before or is closed during this call.
     * @throws UnsupportedOperationException if the underlying technology does not support efficiently reading the last
     *                                       message in the topic.
     */
    MessiCursor cursorHead();

    /**
     * Will return a cursor pointed at the oldest retained message in the stream and will vary with retention policy.
     *
     * @return a cursor pointed at the oldest retained message in the stream
     * @throws MessiClosedException if the topic was closed before or is closed during this call.
     */
    MessiCursor cursorAtTrimHorizon();
}

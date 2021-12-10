package no.cantara.messi.api;

import no.cantara.messi.protos.MessiMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A subscription based consumer that can be used to receive and acknowledge messages on a stream.
 */
public interface MessiStreamingConsumer extends AutoCloseable {

    /**
     * @return the name of the topic from which this consumer will consume messages from.
     */
    String topic();

    /**
     * Receive the next message after the current position, or null if no message is available before the timeout. If
     * successful, the current position for this consumer is updated to that of the returned message.
     *
     * @param timeout the timeout in units as specified by the unit parameter.
     * @param unit    the unit of the timeout, e.g. TimeUnit.SECONDS
     * @return the next available message before the timeout occurs, or null if no next message is available before the
     * timeout.
     * @throws InterruptedException if the calling thread is interrupted while waiting on an available message.
     * @throws MessiClosedException if method is called after this instance has been closed.
     */
    MessiMessage receive(int timeout, TimeUnit unit) throws InterruptedException, MessiClosedException;

    /**
     * Asynchronously receive a message callback when the next message after the current position is available. The
     * current position is also updated to that of the returned message right before calling the callback.
     *
     * @return a CompletableFuture representing the next available message.
     */
    CompletableFuture<? extends MessiMessage> receiveAsync();

    /**
     * Return the current position of the stream as a cursor. The cursor is positioned immediately after the most
     * recently returned message. The returned cursor can be used to create a checkpoint.
     *
     * @return the current position as a cursor.
     */
    MessiCursor currentPosition();

    /**
     * Seek to a specific time in the stream. The stream will be positioned at the first message with matching timestamp
     * or at the message immediately after if no message match the timestamp. The position will be inclusive, so that
     * the next message returned by receive will be the message that the stream is positioned at.
     *
     * @param timestamp the timestamp in milliseconds from epoch (1970)
     */
    void seek(long timestamp);

    /**
     * Get the shard that this consumer belongs to.
     *
     * @return the shard that this consumer belongs to.
     */
    MessiShard shard();

    /**
     * Returns whether or not the consumer is closed.
     *
     * @return whether the consumer is closed.
     */
    boolean isClosed();

    @Override
    void close();
}

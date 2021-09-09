package no.cantara.messi.api;

import no.cantara.messi.protos.MessiMessage;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface MessiProducer extends AutoCloseable {

    /**
     * @return the topic on which this producer will publish messages.
     */
    String topic();

    /**
     * Publish messages. Published content will be assigned an ulid if it is missing from any of the provided messages.
     *
     * @param messages a list of messages
     * @throws MessiClosedException if the producer was closed before or during this call.
     */
    default void publish(List<MessiMessage> messages) throws MessiClosedException {
        publish(messages.toArray(new MessiMessage[0]));
    }

    /**
     * Publish messages. Published content will be assigned an ulid if it is missing from any of the provided messages.
     *
     * @param messages a list of messages
     * @throws MessiClosedException if the producer was closed before or during this call.
     */
    void publish(MessiMessage... messages) throws MessiClosedException;

    /**
     * Asynchronously publish all messages. Published content will be assigned an ulid if it is missing from any of the provided messages.
     *
     * @param messages a list of messages
     * @return a completable futures representing the completeness of the async-function.
     */
    default CompletableFuture<Void> publishAsync(List<MessiMessage> messages) {
        return publishAsync(messages.toArray(new MessiMessage[messages.size()]));
    }

    /**
     * Asynchronously publish all messages. Published content will be assigned an ulid if it is missing from any of the provided messages.
     *
     * @param messages a list of messages
     * @return a completable futures representing the completeness of the async-function.
     */
    CompletableFuture<Void> publishAsync(MessiMessage... messages);

    /**
     * Returns whether or not the producer is closed.
     *
     * @return whether the producer is closed.
     */
    boolean isClosed();

    @Override
    void close();

}

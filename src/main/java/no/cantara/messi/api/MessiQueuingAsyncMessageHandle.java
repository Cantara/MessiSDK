package no.cantara.messi.api;

import no.cantara.messi.protos.MessiMessage;

import java.util.concurrent.CompletableFuture;

public interface MessiQueuingAsyncMessageHandle {

    /**
     * Gets the received message.
     *
     * @return the received message.
     */
    MessiMessage message();

    /**
     * Delete a message from the corresponding server-side queue. This will avoid redelivery of this message, and marks
     * overall progression in the queue. All messages consumed must eventually be deleted.
     *
     * @return a {@link CompletableFuture} which will complete after the delete has been completed.
     */
    CompletableFuture<Void> ack();
}

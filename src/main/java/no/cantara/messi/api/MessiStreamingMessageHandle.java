package no.cantara.messi.api;

import no.cantara.messi.protos.MessiMessage;

public interface MessiStreamingMessageHandle {

    /**
     * Gets the received message.
     *
     * @return the received message.
     */
    MessiMessage message();

    /**
     * Get the cursor pointed immediately after this message.
     */
    MessiCursor cursor();
}

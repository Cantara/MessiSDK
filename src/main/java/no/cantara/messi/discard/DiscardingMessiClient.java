package no.cantara.messi.discard;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiMessage;

import java.time.Duration;

public class DiscardingMessiClient implements MessiClient {

    @Override
    public DiscardingMessiProducer producer(String topic) {
        return new DiscardingMessiProducer(topic);
    }

    @Override
    public MessiConsumer consumer(String topic, MessiCursor cursor) {
        return new DiscardingMessiConsumer(topic);
    }

    @Override
    public MessiCursor cursorOf(String topic, ULID.Value ulid, boolean inclusive) {
        return null;
    }

    @Override
    public MessiCursor cursorOf(String topic, String externalId, boolean inclusive, long approxTimestamp, Duration tolerance) {
        return null;
    }

    @Override
    public MessiMessage lastMessage(String topic) throws MessiClosedException {
        return null;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {
    }

    @Override
    public DiscardingMessiMetadataClient metadata(String topic) {
        return new DiscardingMessiMetadataClient(topic);
    }
}

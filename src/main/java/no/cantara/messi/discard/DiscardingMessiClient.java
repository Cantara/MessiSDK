package no.cantara.messi.discard;

import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.protos.MessiMessage;

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
    public DiscardingMessiCursor.Builder cursorOf() {
        return new DiscardingMessiCursor.Builder();
    }

    @Override
    public MessiMessage lastMessage(String topic, String shardId) throws MessiClosedException {
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

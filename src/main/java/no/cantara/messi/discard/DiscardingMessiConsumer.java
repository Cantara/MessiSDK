package no.cantara.messi.discard;

import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class DiscardingMessiConsumer implements MessiConsumer {

    final String topic;

    DiscardingMessiConsumer(String topic) {
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public MessiMessage receive(int timeout, TimeUnit unit) throws MessiClosedException {
        return null;
    }

    static final CompletableFuture<MessiMessage> COMPLETED = CompletableFuture.completedFuture(null);

    @Override
    public CompletableFuture<MessiMessage> receiveAsync() {
        return COMPLETED;
    }

    @Override
    public void seek(long timestamp) {
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {
    }
}

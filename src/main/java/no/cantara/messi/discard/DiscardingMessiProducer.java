package no.cantara.messi.discard;

import no.cantara.messi.api.MessiMessage;
import no.cantara.messi.api.MessiProducer;

import java.util.concurrent.CompletableFuture;

class DiscardingMessiProducer implements MessiProducer {

    final String topic;

    DiscardingMessiProducer(String topic) {
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public void publish(MessiMessage... messages) {
    }

    static final CompletableFuture<Void> COMPLETED = CompletableFuture.completedFuture(null);

    @Override
    public CompletableFuture<Void> publishAsync(MessiMessage... messages) {
        return COMPLETED;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {
    }
}

package no.cantara.messi.discard;

import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiStreamingConsumer;
import no.cantara.messi.protos.MessiMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DiscardingMessiStreamingConsumer implements MessiStreamingConsumer {

    final String topicName;

    public DiscardingMessiStreamingConsumer(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public String topic() {
        return topicName;
    }

    @Override
    public MessiMessage receive(int timeout, TimeUnit unit) throws InterruptedException, MessiClosedException {
        return null;
    }

    static final CompletableFuture<MessiMessage> COMPLETED = CompletableFuture.completedFuture(null);

    @Override
    public CompletableFuture<? extends MessiMessage> receiveAsync() {
        return COMPLETED;
    }

    @Override
    public MessiCursor currentPosition() {
        return null;
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

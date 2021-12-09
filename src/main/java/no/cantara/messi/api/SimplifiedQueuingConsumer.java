package no.cantara.messi.api;

import no.cantara.messi.protos.MessiMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class SimplifiedQueuingConsumer implements MessiConsumer {

    final MessiShard shard;
    final MessiQueuingConsumer queuingConsumer;

    public SimplifiedQueuingConsumer(MessiShard shard, MessiQueuingConsumer queuingConsumer) {
        this.shard = shard;
        this.queuingConsumer = queuingConsumer;
    }

    @Override
    public String topic() {
        return queuingConsumer.topic();
    }

    @Override
    public MessiMessage receive(int timeout, TimeUnit unit) throws InterruptedException, MessiClosedException {
        MessiQueuingMessageHandle handle = queuingConsumer.receive(timeout, unit);
        handle.ack(); // NOTE: This is very likely to lose messages at some point.
        return handle.message();
    }

    @Override
    public CompletableFuture<? extends MessiMessage> receiveAsync() {
        return queuingConsumer.receiveAsync()
                .thenApply(handle -> {
                    handle.ack(); // NOTE: This is very likely to lose messages at some point.
                    return handle;
                })
                .thenApply(MessiQueuingAsyncMessageHandle::message);
    }

    @Override
    public void seek(long timestamp) {
        throw new UnsupportedOperationException("Not possible to seek on a queueing consumer");
    }

    @Override
    public MessiCursor cursorAt(MessiMessage message) {
        throw new UnsupportedOperationException("Not possible to get cursor on a queueing topic, the position is fully managed on the server-side.");
    }

    @Override
    public MessiCursor cursorAfter(MessiMessage message) {
        throw new UnsupportedOperationException("Not possible to get cursor on a queueing topic, the position is fully managed on the server-side.");
    }

    @Override
    public boolean isClosed() {
        return queuingConsumer.isClosed();
    }

    @Override
    public void close() {
        queuingConsumer.close();
    }
}

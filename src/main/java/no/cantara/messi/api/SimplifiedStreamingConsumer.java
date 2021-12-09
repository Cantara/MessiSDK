package no.cantara.messi.api;

import no.cantara.messi.protos.MessiMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class SimplifiedStreamingConsumer implements MessiConsumer {

    final MessiShard shard;
    final MessiStreamingConsumer streamingConsumer;

    public SimplifiedStreamingConsumer(MessiShard shard, MessiStreamingConsumer streamingConsumer) {
        this.shard = shard;
        this.streamingConsumer = streamingConsumer;
    }

    @Override
    public String topic() {
        return streamingConsumer.topic();
    }

    @Override
    public MessiMessage receive(int timeout, TimeUnit unit) throws InterruptedException, MessiClosedException {
        return streamingConsumer.receive(timeout, unit);
    }

    @Override
    public CompletableFuture<? extends MessiMessage> receiveAsync() {
        return streamingConsumer.receiveAsync();
    }

    @Override
    public void seek(long timestamp) {
        streamingConsumer.seek(timestamp);
    }

    @Override
    public MessiCursor cursorAt(MessiMessage message) {
        return shard.cursorAt(message);
    }

    @Override
    public MessiCursor cursorAfter(MessiMessage message) {
        return shard.cursorAfter(message);
    }

    @Override
    public boolean isClosed() {
        return streamingConsumer.isClosed();
    }

    @Override
    public void close() {
        streamingConsumer.close();
    }
}

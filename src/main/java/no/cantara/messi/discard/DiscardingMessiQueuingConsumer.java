package no.cantara.messi.discard;

import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiQueuingAsyncMessageHandle;
import no.cantara.messi.api.MessiQueuingConsumer;
import no.cantara.messi.api.MessiQueuingMessageHandle;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DiscardingMessiQueuingConsumer implements MessiQueuingConsumer {

    final DiscardingMessiShard shard;

    public DiscardingMessiQueuingConsumer(DiscardingMessiShard shard) {
        this.shard = shard;
    }

    @Override
    public String topic() {
        return null;
    }

    @Override
    public MessiQueuingMessageHandle receive(int timeout, TimeUnit unit) throws InterruptedException, MessiClosedException {
        return null;
    }

    @Override
    public CompletableFuture<? extends MessiQueuingAsyncMessageHandle> receiveAsync() {
        return null;
    }

    @Override
    public DiscardingMessiShard shard() {
        return shard;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {

    }
}

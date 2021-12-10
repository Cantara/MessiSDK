package no.cantara.messi.discard;

import no.cantara.messi.api.MessiProducer;
import no.cantara.messi.api.MessiTopic;

public class DiscardingMessiTopic implements MessiTopic {

    final DiscardingMessiClient client;
    final String name;

    DiscardingMessiTopic(DiscardingMessiClient client, String name) {
        this.client = client;
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public MessiProducer producer() {
        return new DiscardingMessiProducer(name);
    }

    @Override
    public DiscardingMessiShard shardOf(String shardId) {
        return new DiscardingMessiShard(this);
    }

    @Override
    public DiscardingMessiMetadataClient metadata() {
        return new DiscardingMessiMetadataClient(name);
    }

    @Override
    public DiscardingMessiClient client() {
        return client;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {
    }
}

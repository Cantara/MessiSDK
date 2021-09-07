package no.cantara.messi.discard;

import no.cantara.messi.api.MessiMetadataClient;

import java.util.Collections;
import java.util.Set;

public class DiscardingMessiMetadataClient implements MessiMetadataClient {

    final String topic;

    public DiscardingMessiMetadataClient(String topic) {
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Set<String> keys() {
        return Collections.emptySet();
    }

    @Override
    public byte[] get(String key) {
        return null;
    }

    @Override
    public DiscardingMessiMetadataClient put(String key, byte[] value) {
        return this;
    }

    @Override
    public DiscardingMessiMetadataClient remove(String key) {
        return this;
    }
}

package no.cantara.messi.memory;

import no.cantara.messi.api.MessiMetadataClient;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryMessiMetadataClient implements MessiMetadataClient {

    final String topic;
    final Map<String, byte[]> map = new ConcurrentHashMap<>();

    public MemoryMessiMetadataClient(String topic) {
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Set<String> keys() {
        return map.keySet();
    }

    @Override
    public byte[] get(String key) {
        return map.get(key);
    }

    @Override
    public MemoryMessiMetadataClient put(String key, byte[] value) {
        map.put(key, value);
        return this;
    }

    @Override
    public MemoryMessiMetadataClient remove(String key) {
        map.remove(key);
        return this;
    }
}

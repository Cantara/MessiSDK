package no.cantara.messi.api;

import de.huxhorn.sulky.ulid.ULID;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

class MessiDefaultMessage implements MessiMessage {

    final ULID.Value ulid;
    final String clientSourceId;
    final long providerPublishedTimestamp;
    final String providerShardId;
    final String providerSequenceNumber;
    final String partitionKey;
    final String orderingGroup;
    final long sequenceNumber;
    final String externalId;
    final Map<String, String> attributes;
    final Map<String, byte[]> data;

    MessiDefaultMessage(ULID.Value ulid, String clientSourceId, long providerPublishedTimestamp, String providerShardId, String providerSequenceNumber, String partitionKey, String orderingGroup, long sequenceNumber, String externalId, Map<String, String> attributes, Map<String, byte[]> data) {
        if (externalId == null) {
            throw new IllegalArgumentException("position cannot be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        this.ulid = ulid;
        this.clientSourceId = clientSourceId;
        this.providerPublishedTimestamp = providerPublishedTimestamp;
        this.providerShardId = providerShardId;
        this.providerSequenceNumber = providerSequenceNumber;
        this.partitionKey = partitionKey;
        this.orderingGroup = orderingGroup;
        this.sequenceNumber = sequenceNumber;
        this.externalId = externalId;
        this.attributes = attributes;
        this.data = data;
    }

    @Override
    public MessiMessage.Builder copy() {
        return new Builder(this);
    }

    @Override
    public ULID.Value ulid() {
        return ulid;
    }

    @Override
    public String clientSourceId() {
        return clientSourceId;
    }

    @Override
    public long providerPublishedTimestamp() {
        return providerPublishedTimestamp;
    }

    @Override
    public String providerShardId() {
        return providerShardId;
    }

    @Override
    public String providerSequenceNumber() {
        return providerSequenceNumber;
    }

    @Override
    public String partitionKey() {
        return partitionKey;
    }

    @Override
    public String orderingGroup() {
        return orderingGroup;
    }

    @Override
    public long sequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String externalId() {
        return externalId;
    }

    @Override
    public Set<String> keys() {
        return data.keySet();
    }

    @Override
    public byte[] get(String key) {
        return data.get(key);
    }

    @Override
    public Set<String> attributes() {
        return attributes.keySet();
    }

    @Override
    public Map<String, String> attributeMap() {
        return attributes;
    }

    @Override
    public String attribute(String key) {
        return attributes.get(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessiDefaultMessage that = (MessiDefaultMessage) o;
        return ulid.equals(that.ulid) &&
                orderingGroup.equals(that.orderingGroup) &&
                sequenceNumber == that.sequenceNumber &&
                externalId.equals(that.externalId) &&
                this.data.keySet().equals(that.data.keySet()) &&
                this.data.keySet().stream().allMatch(key -> Arrays.equals(this.data.get(key), that.data.get(key)));
    }

    @Override
    public int hashCode() {
        return Objects.hash(ulid, orderingGroup, sequenceNumber, externalId, data);
    }

    @Override
    public String toString() {
        return "MessiDefaultMessage{" +
                "ulid=" + ulid +
                ", orderingGroup='" + orderingGroup + '\'' +
                ", sequenceNumber=" + sequenceNumber +
                ", externalId='" + externalId + '\'' +
                ", data.keys=" + data.keySet() +
                '}';
    }

    static class Builder implements MessiMessage.Builder {
        long providerPublishedTimestamp = -1;
        String clientSourceId;
        String providerShardId;
        String providerSequenceNumber;
        String partitionKey;
        String orderingGroup;
        ULID.Value ulid;
        long sequenceNumber = -1;
        String externalId;
        final Map<String, String> attributes = new LinkedHashMap<>();
        final Map<String, byte[]> data = new LinkedHashMap<>();

        Builder() {
        }

        Builder(MessiDefaultMessage source) {
            providerPublishedTimestamp = source.providerPublishedTimestamp;
            clientSourceId = source.clientSourceId;
            providerShardId = source.providerShardId;
            providerSequenceNumber = source.providerSequenceNumber;
            partitionKey = source.partitionKey;
            orderingGroup = source.orderingGroup;
            ulid = source.ulid;
            sequenceNumber = source.sequenceNumber;
            externalId = source.externalId;
            attributes.putAll(source.attributes);
            data.putAll(source.data);
        }

        @Override
        public MessiMessage.Builder ulid(ULID.Value ulid) {
            this.ulid = ulid;
            return this;
        }

        @Override
        public MessiMessage.Builder clientSourceId(String clientSourceId) {
            this.clientSourceId = clientSourceId;
            return this;
        }

        @Override
        public MessiMessage.Builder providerShardId(String providerShardId) {
            this.providerShardId = providerShardId;
            return this;
        }

        @Override
        public MessiMessage.Builder providerSequenceNumber(String providerSequenceNumber) {
            this.providerSequenceNumber = providerSequenceNumber;
            return this;
        }

        @Override
        public MessiMessage.Builder providerPublishedTimestamp(long providerPublishedTimestamp) {
            this.providerPublishedTimestamp = providerPublishedTimestamp;
            return this;
        }

        @Override
        public MessiMessage.Builder partitionKey(String partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        @Override
        public MessiMessage.Builder orderingGroup(String orderingGroup) {
            this.orderingGroup = orderingGroup;
            return this;
        }

        @Override
        public MessiMessage.Builder sequenceNumber(long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
            return this;
        }

        @Override
        public MessiMessage.Builder externalId(String externalId) {
            this.externalId = externalId;
            return this;
        }

        @Override
        public MessiMessage.Builder attribute(String key, String value) {
            attributes.put(key, value);
            return this;
        }

        @Override
        public MessiMessage.Builder put(String key, byte[] payload) {
            data.put(key, payload);
            return this;
        }

        @Override
        public MessiDefaultMessage build() {
            return new MessiDefaultMessage(ulid, clientSourceId, providerPublishedTimestamp, providerShardId, providerSequenceNumber, partitionKey, orderingGroup, sequenceNumber, externalId, attributes, data);
        }
    }
}

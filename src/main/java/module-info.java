module messi.sdk {
    requires property.config;
    requires org.slf4j;
    requires com.google.protobuf;
    requires de.huxhorn.sulky.ulid;

    provides no.cantara.messi.api.MessiClientFactory with
            no.cantara.messi.discard.DiscardingMessiClientFactory,
            no.cantara.messi.memory.MemoryMessiClientFactory;

    exports no.cantara.messi.api;
    exports no.cantara.messi.protos;
    exports no.cantara.messi.discard;
    exports no.cantara.messi.memory;
}

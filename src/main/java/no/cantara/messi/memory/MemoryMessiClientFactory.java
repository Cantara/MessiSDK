package no.cantara.messi.memory;

import no.cantara.config.ApplicationProperties;
import no.cantara.messi.api.MessiClientFactory;

public class MemoryMessiClientFactory implements MessiClientFactory {

    @Override
    public Class<?> providerClass() {
        return MemoryMessiClient.class;
    }

    @Override
    public String alias() {
        return "memory";
    }

    @Override
    public MemoryMessiClient create(ApplicationProperties applicationProperties) {
        return new MemoryMessiClient();
    }
}

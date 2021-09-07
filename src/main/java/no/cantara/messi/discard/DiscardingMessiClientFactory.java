package no.cantara.messi.discard;

import no.cantara.config.ApplicationProperties;
import no.cantara.messi.api.MessiClientFactory;

public class DiscardingMessiClientFactory implements MessiClientFactory {

    @Override
    public Class<?> providerClass() {
        return DiscardingMessiClient.class;
    }

    @Override
    public String alias() {
        return "discard";
    }

    @Override
    public DiscardingMessiClient create(ApplicationProperties applicationProperties) {
        return new DiscardingMessiClient();
    }
}

package no.cantara.messi.api;

import no.cantara.config.ApplicationProperties;
import no.cantara.config.ProviderLoader;
import no.cantara.messi.discard.DiscardingMessiClient;
import no.cantara.messi.memory.MemoryMessiClient;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MessiClientProviderTest {

    @Test
    public void thatMemoryAndNoneMessiClientsAreAvailableThroughServiceProviderMechanism() {
        ApplicationProperties applicationProperties = ApplicationProperties.builder().testDefaults().build();
        {
            MessiClient client = ProviderLoader.configure(applicationProperties, "memory", MessiClientFactory.class);
            assertNotNull(client);
            assertTrue(client instanceof MemoryMessiClient);
        }
        {
            MessiClient client = ProviderLoader.configure(applicationProperties, "discard", MessiClientFactory.class);
            assertNotNull(client);
            assertTrue(client instanceof DiscardingMessiClient);
        }
    }
}

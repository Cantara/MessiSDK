package no.cantara.messi.discard;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.config.ApplicationProperties;
import no.cantara.config.ProviderLoader;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiMessage;
import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.api.MessiProducer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

public class MessiClientTest {

    MessiClient client;

    @BeforeMethod
    public void createMessiClient() {
        ApplicationProperties applicationProperties = ApplicationProperties.builder().testDefaults().build();
        client = ProviderLoader.configure(applicationProperties, "discard", MessiClientFactory.class);
    }

    @AfterMethod
    public void closeMessiClient() throws Exception {
        client.close();
    }

    @Test
    public void thatClientMethodsReturnEmpty() throws Exception {
        assertNull(client.lastMessage("the-topic"));
        assertNull(client.cursorOf("the-topic", null, true));
        assertNull(client.cursorOf("the-topic", "p1", true, 0, Duration.ZERO));
        assertFalse(client.isClosed());
    }

    @Test
    public void thatProducerMethodsAcceptAndDiscardAll() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(MessiMessage.builder()
                    .ulid(new ULID.Value(0, 0))
                    .externalId("p1")
                    .put("k", new byte[0])
                    .build());
            assertEquals(producer.topic(), "the-topic");
            assertFalse(producer.isClosed());
        }
    }

    @Test
    public void thatConsumerReturnEmpty() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(MessiMessage.builder()
                    .ulid(new ULID.Value(0, 0))
                    .externalId("p1")
                    .put("k", new byte[0])
                    .build());
        }
        try (MessiConsumer consumer = client.consumer("the-topic")) {
            assertNull(consumer.receive(0, TimeUnit.MILLISECONDS));
            assertNull(consumer.receiveAsync().join());
            assertEquals(consumer.topic(), "the-topic");
            consumer.seek(123);
            assertFalse(consumer.isClosed());
        }
    }

    @Test
    public void thatMetadataCanBeWrittenListedAndRead() {
        MessiMetadataClient metadata = client.metadata("the-topic");
        assertEquals(metadata.topic(), "the-topic");
        assertEquals(metadata.keys().size(), 0);
        metadata.put("key-1", "Value-1".getBytes(StandardCharsets.UTF_8));
        assertEquals(metadata.keys().size(), 0);
        assertNull(metadata.get("key-1"));
    }
}

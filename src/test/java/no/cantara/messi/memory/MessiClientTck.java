package no.cantara.messi.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.config.ApplicationProperties;
import no.cantara.config.ProviderLoader;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiMessage;
import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.api.MessiNoSuchExternalIdException;
import no.cantara.messi.api.MessiProducer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class MessiClientTck {

    MessiClient client;

    @BeforeMethod
    public void createMessiClient() {
        ApplicationProperties applicationProperties = ApplicationProperties.builder().testDefaults().build();
        client = ProviderLoader.configure(applicationProperties, "memory", MessiClientFactory.class);
    }

    @AfterMethod
    public void closeMessiClient() throws Exception {
        client.close();
    }

    @Test
    public void thatLastExternalIdOfEmptyTopicCanBeReadAndIsNull() {
        assertNull(client.lastMessage("the-topic"));
    }

    @Test
    public void thatLastPositionOfProducerCanBeRead() {
        MessiProducer producer = client.producer("the-topic");

        producer.publish(
                MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build()
        );

        assertEquals(client.lastMessage("the-topic").externalId(), "b");

        producer.publish(MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build());

        assertEquals(client.lastMessage("the-topic").externalId(), "c");
    }

    @Test
    public void thatAllFieldsOfMessageSurvivesStream() throws Exception {
        ULID.Value ulid = new ULID().nextValue();
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder()
                            .ulid(ulid)
                            .orderingGroup("og1")
                            .sequenceNumber(1)
                            .externalId("a")
                            .put("payload1", new byte[3])
                            .put("payload2", new byte[7])
                            .build(),
                    MessiMessage.builder()
                            .orderingGroup("og1")
                            .sequenceNumber(2)
                            .externalId("b")
                            .put("payload1", new byte[4])
                            .put("payload2", new byte[8])
                            .build(),
                    MessiMessage.builder()
                            .orderingGroup("og1")
                            .sequenceNumber(3)
                            .externalId("c")
                            .put("payload1", new byte[2])
                            .put("payload2", new byte[5])
                            .providerPublishedTimestamp(123)
                            .providerShardId("shardId123")
                            .providerSequenceNumber("three")
                            .clientSourceId("client-source-id-123")
                            .attribute("key1", "value1")
                            .attribute("some-other-key", "some other value")
                            .attribute("iamanattribute", "yes I am")
                            .build()
            );
        }

        try (MessiConsumer consumer = client.consumer("the-topic", ulid, true)) {
            {
                MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertEquals(message.ulid(), ulid);
                assertEquals(message.orderingGroup(), "og1");
                assertEquals(message.sequenceNumber(), 1);
                assertEquals(message.externalId(), "a");
                assertEquals(message.keys().size(), 2);
                assertEquals(message.get("payload1"), new byte[3]);
                assertEquals(message.get("payload2"), new byte[7]);
            }
            {
                MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertNotNull(message.ulid());
                assertEquals(message.orderingGroup(), "og1");
                assertEquals(message.sequenceNumber(), 2);
                assertEquals(message.externalId(), "b");
                assertEquals(message.keys().size(), 2);
                assertEquals(message.get("payload1"), new byte[4]);
                assertEquals(message.get("payload2"), new byte[8]);
            }
            {
                MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertNotNull(message.ulid());
                assertEquals(message.orderingGroup(), "og1");
                assertEquals(message.sequenceNumber(), 3);
                assertEquals(message.externalId(), "c");
                assertEquals(message.keys().size(), 2);
                assertTrue(message.keys().contains("payload1"));
                assertTrue(message.keys().contains("payload2"));
                assertEquals(message.get("payload1"), new byte[2]);
                assertEquals(message.get("payload2"), new byte[5]);
                assertEquals(message.data().size(), 2);
                assertEquals(message.data().get("payload1"), new byte[2]);
                assertEquals(message.data().get("payload2"), new byte[5]);
                assertEquals(message.providerPublishedTimestamp(), 123);
                assertEquals(message.providerShardId(), "shardId123");
                assertEquals(message.providerSequenceNumber(), "three");
                assertEquals(message.clientSourceId(), "client-source-id-123");
                assertEquals(message.attributes().size(), 3);
                assertTrue(message.attributes().contains("key1"));
                assertTrue(message.attributes().contains("some-other-key"));
                assertTrue(message.attributes().contains("iamanattribute"));
                assertEquals(message.attributes().size(), 3);
                assertEquals(message.attributeMap().size(), 3);
                assertEquals(message.attributeMap().get("key1"), "value1");
                assertEquals(message.attributeMap().get("some-other-key"), "some other value");
                assertEquals(message.attributeMap().get("iamanattribute"), "yes I am");
                assertEquals(message.attribute("key1"), "value1");
                assertEquals(message.attribute("some-other-key"), "some other value");
                assertEquals(message.attribute("iamanattribute"), "yes I am");
            }
        }
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        MessiProducer producer = client.producer("the-topic");
        MessiConsumer consumer = client.consumer("the-topic");

        producer.publish(MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build());

        MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message.externalId(), "a");
        assertEquals(message.keys().size(), 2);
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerAsynchronously() {
        MessiProducer producer = client.producer("the-topic");
        MessiConsumer consumer = client.consumer("the-topic");

        CompletableFuture<? extends MessiMessage> future = consumer.receiveAsync();

        producer.publish(MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build());

        MessiMessage message = future.join();
        assertEquals(message.externalId(), "a");
        assertEquals(message.keys().size(), 2);
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        MessiProducer producer = client.producer("the-topic");
        MessiConsumer consumer = client.consumer("the-topic");

        producer.publish(
                MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
        );

        MessiMessage message1 = consumer.receive(1, TimeUnit.SECONDS);
        MessiMessage message2 = consumer.receive(1, TimeUnit.SECONDS);
        MessiMessage message3 = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message1.externalId(), "a");
        assertEquals(message2.externalId(), "b");
        assertEquals(message3.externalId(), "c");
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerAsynchronously() {
        MessiProducer producer = client.producer("the-topic");
        MessiConsumer consumer = client.consumer("the-topic");

        CompletableFuture<List<MessiMessage>> future = receiveAsyncAddMessageAndRepeatRecursive(consumer, "c", new ArrayList<>());

        producer.publish(
                MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
        );

        List<MessiMessage> messages = future.join();

        assertEquals(messages.get(0).externalId(), "a");
        assertEquals(messages.get(1).externalId(), "b");
        assertEquals(messages.get(2).externalId(), "c");
    }

    private CompletableFuture<List<MessiMessage>> receiveAsyncAddMessageAndRepeatRecursive(MessiConsumer consumer, String endExternalId, List<MessiMessage> messages) {
        return consumer.receiveAsync().thenCompose(message -> {
            messages.add(message);
            if (endExternalId.equals(message.externalId())) {
                return CompletableFuture.completedFuture(messages);
            }
            return receiveAsyncAddMessageAndRepeatRecursive(consumer, endExternalId, messages);
        });
    }

    @Test
    public void thatMessagesCanBeConsumedByMultipleConsumers() {
        MessiProducer producer = client.producer("the-topic");
        MessiConsumer consumer1 = client.consumer("the-topic");
        MessiConsumer consumer2 = client.consumer("the-topic");

        CompletableFuture<List<MessiMessage>> future1 = receiveAsyncAddMessageAndRepeatRecursive(consumer1, "c", new ArrayList<>());
        CompletableFuture<List<MessiMessage>> future2 = receiveAsyncAddMessageAndRepeatRecursive(consumer2, "c", new ArrayList<>());

        producer.publish(MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
        );

        List<MessiMessage> messages1 = future1.join();
        assertEquals(messages1.get(0).externalId(), "a");
        assertEquals(messages1.get(1).externalId(), "b");
        assertEquals(messages1.get(2).externalId(), "c");

        List<MessiMessage> messages2 = future2.join();
        assertEquals(messages2.get(0).externalId(), "a");
        assertEquals(messages2.get(1).externalId(), "b");
        assertEquals(messages2.get(2).externalId(), "c");
    }

    @Test
    public void thatConsumerCanReadFromBeginning() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build(),
                    MessiMessage.builder().externalId("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic")) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.externalId(), "a");
        }
    }

    @Test
    public void thatConsumerCanReadFromFirstMessage() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build(),
                    MessiMessage.builder().externalId("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic", "a", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.externalId(), "b");
        }
    }

    @Test
    public void thatConsumerCanReadFromMiddle() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build(),
                    MessiMessage.builder().externalId("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic", "b", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.externalId(), "c");
        }
        try (MessiConsumer consumer = client.consumer("the-topic", "c", true, System.currentTimeMillis(), Duration.ofMinutes(1))) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.externalId(), "c");
        }
    }

    @Test
    public void thatConsumerCanReadFromRightBeforeLast() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build(),
                    MessiMessage.builder().externalId("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic", "c", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.externalId(), "d");
        }
    }

    @Test
    public void thatConsumerCanReadFromLast() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build(),
                    MessiMessage.builder().externalId("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic", "d", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            MessiMessage message = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(message);
        }
    }

    @Test
    public void thatSeekToWorks() throws Exception {
        long timestampBeforeA;
        long timestampBeforeB;
        long timestampBeforeC;
        long timestampBeforeD;
        long timestampAfterD;
        try (MessiProducer producer = client.producer("the-topic")) {
            timestampBeforeA = System.currentTimeMillis();
            producer.publish(MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build());
            Thread.sleep(5);
            timestampBeforeB = System.currentTimeMillis();
            producer.publish(MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build());
            Thread.sleep(5);
            timestampBeforeC = System.currentTimeMillis();
            producer.publish(MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build());
            Thread.sleep(5);
            timestampBeforeD = System.currentTimeMillis();
            producer.publish(MessiMessage.builder().externalId("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build());
            Thread.sleep(5);
            timestampAfterD = System.currentTimeMillis();
        }
        try (MessiConsumer consumer = client.consumer("the-topic")) {
            consumer.seek(timestampAfterD);
            assertNull(consumer.receive(100, TimeUnit.MILLISECONDS));
            consumer.seek(timestampBeforeD);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).externalId(), "d");
            consumer.seek(timestampBeforeB);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).externalId(), "b");
            consumer.seek(timestampBeforeC);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).externalId(), "c");
            consumer.seek(timestampBeforeA);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).externalId(), "a");
        }
    }

    @Test
    public void thatCursorOfValidExternalIdIsFound() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        assertNotNull(client.cursorOf("the-topic", "a", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
        assertNotNull(client.cursorOf("the-topic", "b", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
        assertNotNull(client.cursorOf("the-topic", "c", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
    }

    @Test(expectedExceptions = MessiNoSuchExternalIdException.class)
    public void thatCursorOfInvalidExternalIdIsNotFound() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        assertNull(client.cursorOf("the-topic", "d", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
    }

    @Test(expectedExceptions = MessiNoSuchExternalIdException.class)
    public void thatCursorOfEmptyTopicIsNotFound() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
        }
        client.cursorOf("the-topic", "d", true, System.currentTimeMillis(), Duration.ofMinutes(1));
    }

    @Test
    public void thatMetadataCanBeWrittenListedAndRead() {
        MessiMetadataClient metadata = client.metadata("the-topic");
        assertEquals(metadata.topic(), "the-topic");
        assertEquals(metadata.keys().size(), 0);
        metadata.put("key-1", "Value-1".getBytes(StandardCharsets.UTF_8));
        metadata.put("key-2", "Value-2".getBytes(StandardCharsets.UTF_8));
        assertEquals(metadata.keys().size(), 2);
        assertEquals(new String(metadata.get("key-1"), StandardCharsets.UTF_8), "Value-1");
        assertEquals(new String(metadata.get("key-2"), StandardCharsets.UTF_8), "Value-2");
        metadata.put("key-2", "Overwritten-Value-2".getBytes(StandardCharsets.UTF_8));
        assertEquals(metadata.keys().size(), 2);
        assertEquals(new String(metadata.get("key-2"), StandardCharsets.UTF_8), "Overwritten-Value-2");
        metadata.remove("key-1");
        assertEquals(metadata.keys().size(), 1);
        metadata.remove("key-2");
        assertEquals(metadata.keys().size(), 0);
    }
}

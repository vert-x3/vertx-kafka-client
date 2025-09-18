package io.vertx.kafka.client.tests;

import io.strimzi.test.container.StrimziKafkaCluster;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.runner.RunWith;

/**
 * Base class for tests providing a Kafka cluster using Strimzi Test Container
 */
@RunWith(VertxUnitRunner.class)
public abstract class KafkaStrimziTestBase extends KafkaTestBase {

    // Hold the strimzi kafka cluster instance
    protected static KafkaClusterWrapper kafkaCluster;

    // Hold the client instance to manage Kafka topics and configurations
    protected static AdminClient adminClient;

    protected static boolean ACL = false;

    public static KafkaClusterWrapper kafkaCluster(boolean acl) {
        if (kafkaCluster != null) {
            throw new IllegalStateException();
        }

        Map<String, String> kafkaConfig = new HashMap<>();
        if (acl) {
            kafkaConfig.put("authorizer.class.name", "kafka.security.authorizer.AclAuthorizer");
            kafkaConfig.put("super.users", "User:ANONYMOUS");
        }

        StrimziKafkaCluster strimziCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(2)
                .withKafkaVersion("3.7.1")
                .withKraft()
                .withAdditionalKafkaConfiguration(kafkaConfig)
                .build();

        // We need to create the wrapper in the setUp method where we have an instance
        return new KafkaClusterWrapper(strimziCluster, null);
    }

    @BeforeClass
    public static void setUp() {
        // Create the Kafka cluster
        StrimziKafkaCluster strimziCluster = kafkaCluster(false).getDelegate();

        // Create the wrapper with the current instance
        kafkaCluster = new KafkaClusterWrapper(strimziCluster, new KafkaStrimziTestBase() {
        });
        kafkaCluster.start();

        // Create admin client for topic management
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());
        adminClient = AdminClient.create(adminProps);
    }

    /**
     * Stops the cluster if it exists
     */
    @AfterClass
    public static void tearDown() {
        if (adminClient != null) {
            adminClient.close();
            adminClient = null;
        }

        if (kafkaCluster != null) {
            kafkaCluster.stop();
            kafkaCluster = null;
        }
    }

    @Test
    public void dummy() {
        // 'No runnable methods' is thrown without this
    }

    /**
     * Create a topic with the specified name, partitions, and replication factor
     */
    public static void createTopic(String topicName, int partitions, int replicationFactor) {
        try {
            NewTopic newTopic = new NewTopic(topicName, partitions, (short) replicationFactor);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get(30, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to create topic " + topicName, e);
        }
    }

    /**
     * Create multiple topics
     */
    public static void createTopics(Set<String> topics) {
        topics.forEach(topic -> createTopic(topic, 1, 1));
    }

    /**
     * Helper class to provide access to Kafka operations
     */
    public KafkaTestHelper useTo() {
        return new KafkaTestHelper();
    }

    /**
     * Helper class for Kafka operations
     */
    public class KafkaTestHelper {
        /**
         * Get consumer properties for the specified consumer group and client ID
         */
        public Properties getConsumerProperties(String groupId, String clientId,
                OffsetResetStrategy offsetResetStrategy) {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy.name().toLowerCase());
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            return props;
        }

        /**
         * Get producer properties for the specified client ID
         */
        public Properties getProducerProperties(String clientId) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());
            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            return props;
        }

        /**
         * Produce strings to a topic
         */
        public void produceStrings(int messageCount, Runnable completionCallback,
                Supplier<ProducerRecord<String, String>> recordSupplier) {
            Properties props = getProducerProperties("producer-" + UUID.randomUUID());
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(
                    props)) {
                for (int i = 0; i < messageCount; i++) {
                    producer.send(recordSupplier.get());
                }
                producer.flush();

                if (completionCallback != null) {
                    completionCallback.run();
                }
            }
        }

        /**
         * Produce integers to a topic
         */
        public void produceIntegers(String topic, int messageCount, int startingOffset, Runnable completionCallback) {
            Properties props = getProducerProperties("producer-" + UUID.randomUUID());
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

            try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(
                    props)) {
                for (int i = 0; i < messageCount; i++) {
                    producer.send(new ProducerRecord<>(topic, 0, null, startingOffset + i));
                }
                producer.flush();

                if (completionCallback != null) {
                    completionCallback.run();
                }
            }
        }

        public <K, V> void consume(String groupId, String clientId, OffsetResetStrategy offsetResetStrategy,
                Deserializer<K> keyDeserializer,
                Deserializer<V> valueDeserializer,
                Supplier<Boolean> continuePolling,
                OffsetCommitCallback commitCallback,
                Runnable completionCallback,
                Collection<String> topics,
                Consumer<ConsumerRecord<K, V>> recordConsumer) {
            Properties props = getConsumerProperties(groupId, clientId, offsetResetStrategy);

            Thread consumerThread = new Thread(() -> {
                try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(props, keyDeserializer, valueDeserializer)) {
                    consumer.subscribe(topics);

                    while (continuePolling.get()) {
                        ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
                        for (ConsumerRecord<K, V> record : records) {
                            recordConsumer.accept(record);
                        }

                        if (commitCallback != null) {
                            consumer.commitAsync(commitCallback);
                        } else {
                            consumer.commitAsync();
                        }
                    }

                    if (completionCallback != null) {
                        completionCallback.run();
                    }
                }
            });

            consumerThread.start();
        }

        /**
         * Consume string messages from a topic
         */
        public void consumeStrings(String topic, int expectedMessageCount, long timeout, TimeUnit unit,
                Runnable completionCallback) {
            CountDownLatch latch = new CountDownLatch(expectedMessageCount);

            consume(
                    "group-" + UUID.randomUUID(),
                    "consumer-" + UUID.randomUUID(),
                    OffsetResetStrategy.EARLIEST,
                    new StringDeserializer(),
                    new StringDeserializer(),
                    () -> latch.getCount() > 0,
                    null,
                    completionCallback,
                    Collections.singleton(topic),
                    record -> latch.countDown());

            try {
                latch.await(timeout, unit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        /**
         * Consume integer messages from a topic
         */
        public void consumeIntegers(String topic, int expectedMessageCount, long timeout, TimeUnit unit,
                Runnable completionCallback) {
            CountDownLatch latch = new CountDownLatch(expectedMessageCount);

            consume(
                    "group-" + UUID.randomUUID(),
                    "consumer-" + UUID.randomUUID(),
                    OffsetResetStrategy.EARLIEST,
                    new StringDeserializer(),
                    new IntegerDeserializer(),
                    () -> latch.getCount() > 0,
                    null,
                    completionCallback,
                    Collections.singleton(topic),
                    record -> latch.countDown());

            try {
                latch.await(timeout, unit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Wrapper for StrimziKafkaCluster that provides a useTo() method for
     * compatibility
     */
    public static class KafkaClusterWrapper {
        private final StrimziKafkaCluster delegate;
        private final KafkaStrimziTestBase testBase;

        public KafkaClusterWrapper(StrimziKafkaCluster delegate, KafkaStrimziTestBase testBase) {
            this.delegate = delegate;
            this.testBase = testBase;
        }

        /**
         * Provides access to the test helper methods
         */
        public KafkaTestHelper useTo() {
            return testBase.useTo();
        }

        /**
         * Get the delegate StrimziKafkaCluster
         */
        public StrimziKafkaCluster getDelegate() {
            return delegate;
        }

        /**
         * Delegate method for getBootstrapServers
         */
        public String getBootstrapServers() {
            return delegate.getBootstrapServers();
        }

        /**
         * Delegate method for start
         */
        public void start() {
            delegate.start();
        }

        /**
         * Delegate method for stop
         */
        public void stop() {
            delegate.stop();
        }
    }
}

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.AfterClass;
import org.junit.BeforeClass;
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
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
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

    private static final Map<String, KafkaConsumer<?, ?>> activeConsumers = new ConcurrentHashMap<>();
    protected static boolean ACL = false;
    private static final String KAFKA_VERSION = System.getProperty("kafka.version");

    public static KafkaClusterWrapper kafkaCluster(boolean acl, int brokers) {
        if (kafkaCluster != null) {
            throw new IllegalStateException();
        }

        Map<String, String> kafkaConfig = new HashMap<>();
        if (acl) {
            kafkaConfig.put("authorizer.class.name", "org.apache.kafka.metadata.authorizer.StandardAuthorizer");
            kafkaConfig.put("super.users", "User:ANONYMOUS");
        }

        StrimziKafkaCluster strimziCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(brokers)
                .withKafkaVersion(KAFKA_VERSION)
                .withKraft()
                .withAdditionalKafkaConfiguration(kafkaConfig)
                .build();

        // We need to create the wrapper in the setUp method where we have an instance
        return new KafkaClusterWrapper(strimziCluster, null);
    }

    public static KafkaClusterWrapper kafkaCluster(boolean acl) {
        return kafkaCluster(acl, 2);
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
                OffsetResetStrategy autoOffsetReset) {
            if (groupId == null) {
                throw new IllegalArgumentException("The groupId is required");
            } 
            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
            if (autoOffsetReset != null) {
                props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset.toString().toLowerCase());
            }
            if (clientId != null) {
                props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
            }
            return props;
        }

        /**
         * Get producer properties for the specified client ID
         */
        public Properties getProducerProperties(String clientId) {
            Properties props = new Properties();
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());
            props.setProperty(ProducerConfig.ACKS_CONFIG, Integer.toString(1));
            if (clientId != null) {
                props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            }
            return props;
        }

        public <K, V> void produce(String producerName, int messageCount, Serializer<K> keySerializer, Serializer<V> valueSerializer, Runnable completionCallback, Supplier<ProducerRecord<K, V>> messageSupplier) {
            Properties props = this.getProducerProperties(producerName);
            Thread t = new Thread(() -> {
                try {
                    KafkaProducer<K, V> producer = new KafkaProducer<K,V>(props, keySerializer, valueSerializer);

                    try {
                        for(int i = 0; i != messageCount; ++i) {
                            ProducerRecord<K, V> record = (ProducerRecord<K,V>)messageSupplier.get();
                            producer.send(record);
                            producer.flush();
                        }
                    } catch (Throwable producerException) {
                        try {
                            producer.close();
                        } catch (Throwable closeException) {
                            producerException.addSuppressed(closeException);
                        }
                        throw producerException;
                    }

                    producer.close();
                } finally {
                    if (completionCallback != null) {
                        completionCallback.run();
                    }
                }

            });
            t.setName(producerName + "-thread");
            t.start();
        }

        /**
         * Produce strings to a topic
         */
        public void produceStrings(int messageCount, Runnable completionCallback, Supplier<ProducerRecord<String, String>> messageSupplier) {
            Serializer<String> keySer = new StringSerializer();
            String randomId = UUID.randomUUID().toString();
            this.produce(randomId, messageCount, keySer, keySer, completionCallback, messageSupplier);
        }

        /**
         * Produce integers to a topic
         */
        public void produceIntegers(int messageCount, Runnable completionCallback, Supplier<ProducerRecord<String, Integer>> messageSupplier) {
            Serializer<String> keySer = new StringSerializer();
            Serializer<Integer> valSer = new IntegerSerializer();
            String randomId = UUID.randomUUID().toString();
            this.produce(randomId, messageCount, keySer, valSer, completionCallback, messageSupplier);
        }

        public void produceIntegers(String topic, int messageCount, int startingOffset, Runnable completionCallback) {
            Properties props = getProducerProperties("producer-" + UUID.randomUUID());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.IntegerSerializer");

            try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(
                    props)) {

                int topicPartitions = producer.partitionsFor(topic).size();
                for (int i = 0; i < messageCount; i++) {
                    producer.send(new ProducerRecord<>(topic, i % topicPartitions, null, startingOffset + i));
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

                    // Store consumer and subscribe
                    activeConsumers.put(groupId, consumer);
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

                    // Explicitly close the consumer before running the completion callback
                    consumer.close();

                    if (completionCallback != null) {
                        completionCallback.run();
                    }
                }
            });

            consumerThread.start();
        }

        /**
         * Consume string messages from a topic with a custom polling condition
         */
        public void consumeStrings(Supplier<Boolean> continuePolling,
                Runnable completionCallback,
                Collection<String> topics,
                Consumer<ConsumerRecord<String, String>> recordConsumer) {
            consume(
                    "group-" + UUID.randomUUID(),
                    "consumer-" + UUID.randomUUID(),
                    OffsetResetStrategy.EARLIEST,
                    new StringDeserializer(),
                    new StringDeserializer(),
                    continuePolling,
                    null,
                    completionCallback,
                    topics,
                    recordConsumer);
        }
        
        /**
         * Consume string messages from a topic with a specified count and timeout
         */
        public void consumeStrings(String topicName, int count, long timeout, TimeUnit unit, Runnable completionCallback) {
            AtomicLong readCounter = new AtomicLong();
            
            consumeStrings(
                    () -> readCounter.get() < count,
                    completionCallback,
                    Collections.singleton(topicName),
                    record -> readCounter.incrementAndGet());
            
            try {
                // Wait for the specified timeout
                Thread.sleep(unit.toMillis(timeout));
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

        public void createTopic(String topicName, int partitions, int replicationFactor) {
            KafkaStrimziTestBase.createTopic(topicName, partitions, replicationFactor);
        }

        public void createTopics(Set<String> topics) {
            KafkaStrimziTestBase.createTopics(topics);
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
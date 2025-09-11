package io.vertx.kafka.client.tests;

import io.strimzi.test.container.StrimziKafkaCluster;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.runner.RunWith;

/**
 * Base class for tests providing a Kafka cluster using Strimzi Test Container
 */
@RunWith(VertxUnitRunner.class)
public abstract class KafkaStrimziTestBase extends KafkaTestBase {

    // Hold the strimzi kafka cluster instance
    protected static StrimziKafkaCluster kafkaCluster;

    // Hold the client instance to manage Kafka topics and configurations
    protected static AdminClient adminClient;

    protected static boolean ACL = false;

    public static StrimziKafkaCluster kafkaCluster(boolean acl) {
        if (kafkaCluster != null) {
            throw new IllegalStateException();
        }

        Map<String, String> kafkaConfig = new HashMap<>();
        if (acl) {
            kafkaConfig.put("authorizer.class.name", "kafka.security.authorizer.AclAuthorizer");
            kafkaConfig.put("super.users", "User:ANONYMOUS");
        }

        // Create 3 node Kafka cluster
        kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .withKafkaVersion("3.7.1")
            .withAdditionalKafkaConfiguration(kafkaConfig)
            .build();

        return kafkaCluster;
    }

    @BeforeClass
    public static void setUp() {
        // Create and start the Kafka cluster
        kafkaCluster = kafkaCluster(false);
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
     * Get consumer properties for the specified consumer group and client ID
     */
    public Properties getConsumerProperties(String groupId, String clientId, OffsetResetStrategy offsetResetStrategy) {
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
}

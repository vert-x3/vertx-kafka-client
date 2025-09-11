package io.vertx.kafka.client.tests;

import io.strimzi.test.container.StrimziKafkaCluster;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
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

}
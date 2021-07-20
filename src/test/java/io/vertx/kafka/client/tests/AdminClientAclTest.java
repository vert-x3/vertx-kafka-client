package io.vertx.kafka.client.tests;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.admin.AccessControlEntryFilter;
import io.vertx.kafka.admin.AclBindingFilter;
import io.vertx.kafka.admin.AclOperation;
import io.vertx.kafka.admin.AclPermissionType;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.PatternType;
import io.vertx.kafka.admin.ResourcePatternFilter;
import io.vertx.kafka.admin.ResourceType;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class AdminClientAclTest extends KafkaClusterTestBase {
    private Vertx vertx;
    private Properties config;
    protected static boolean ACL = true;

    private static Set<String> topics = new HashSet<>();

    static {
        topics.add("first-topic");
        topics.add("second-topic");
//    topics.add("offsets-topic");
    }

    @Before
    public void beforeTest() {
        this.vertx = Vertx.vertx();
        this.config = new Properties();
        this.config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    }

    @After
    public void afterTest(TestContext ctx) {
        this.vertx.close(ctx.asyncAssertSuccess());
    }

    @BeforeClass
    public static void setUp() throws IOException {
        kafkaCluster = kafkaCluster(true).deleteDataPriorToStartup(true).addBrokers(2).startup();
        kafkaCluster.createTopics(topics);
    }

    @Test
    public void testDescribeAcl(TestContext ctx) {
        KafkaAdminClient adminClient = KafkaAdminClient.create(this.vertx, config);
        ResourcePatternFilter rpf = new ResourcePatternFilter(ResourceType.TOPIC, "test-acl-topic", PatternType.LITERAL);
        AccessControlEntryFilter acef = new AccessControlEntryFilter("User:*", "localhost:9092", AclOperation.DESCRIBE, AclPermissionType.ALLOW);
        adminClient.describeAcls(new AclBindingFilter(rpf, acef), ctx.asyncAssertSuccess(list -> {
            ctx.assertTrue(list.isEmpty());
            adminClient.close();
        }));
    }
}

package io.vertx.kafka.client.tests;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.admin.AdminUtils;

public class AdminUtilsTest extends KafkaClusterTestBase {
  private Vertx vertx;
  private String zookeeperHosts = "localhost:2181";

  @Before
  public void beforeTest() {
    vertx = Vertx.vertx();
  }

  @After
  public void afterTest(TestContext ctx) {
    vertx.close(ctx.asyncAssertSuccess());
  }

  @Test
  public void testCreateTopic(TestContext ctx) throws Exception {
    final String topicName = "testCreateTopic";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    AdminUtils adminUtils = AdminUtils.create(vertx, zookeeperHosts, false);

    Async createAsync = ctx.async();

    adminUtils.createTopic(topicName, 1, 1,
      ctx.asyncAssertSuccess(
      res -> createAsync.complete())
    );

    createAsync.awaitSuccess(10000);

    Async deleteAsync = ctx.async();
    adminUtils.deleteTopic(topicName, ctx.asyncAssertSuccess(res -> deleteAsync.complete()));
    deleteAsync.awaitSuccess(10000);
  }

  @Test
  public void testCreateTopicWithTooManyReplicas(TestContext ctx) throws Exception {
    final String topicName = "testCreateTopicWithTooManyReplicas";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    Async async = ctx.async();

    AdminUtils adminUtils = AdminUtils.create(vertx, zookeeperHosts, true);

    adminUtils.createTopic(topicName, 1, 2,
      ctx.asyncAssertFailure(
        res -> {
          ctx.assertEquals("replication factor: 2 larger than available brokers: 1", res.getLocalizedMessage(),
            "Topic creation must fail: only one Broker present, but two replicas requested");
          async.complete();
        })
    );

    async.awaitSuccess(10000);
  }

  @Test
  public void testCreateExistingTopic(TestContext ctx) throws Exception {
    final String topicName = "testCreateExistingTopic";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    Async async = ctx.async();

    AdminUtils adminUtils = AdminUtils.create(vertx, zookeeperHosts, false);

    adminUtils.createTopic(topicName, 1, 1,
      ctx.asyncAssertSuccess(
        res -> async.complete())
    );

    async.awaitSuccess(10000);

    Async create2Async = ctx.async();
    adminUtils.createTopic(topicName, 1, 1,
      ctx.asyncAssertFailure(
        res -> {
          ctx.assertEquals("Topic '"+topicName+"' already exists.", res.getLocalizedMessage(),
            "Topic must already exist");
          create2Async.complete();
        }));

    create2Async.awaitSuccess(10000);

    Async deleteAsync = ctx.async();
    adminUtils.deleteTopic(topicName, ctx.asyncAssertSuccess(res -> deleteAsync.complete()));
    deleteAsync.awaitSuccess(10000);
  }

  @Test
  public void testTopicExists(TestContext ctx) throws Exception {
    final String topicName = "testTopicExists";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    Async createAsync = ctx.async();

    AdminUtils adminUtils = AdminUtils.create(vertx, zookeeperHosts, false);

    adminUtils.createTopic(topicName, 2, 1,
      ctx.asyncAssertSuccess(
        res -> createAsync.complete())
    );

    createAsync.awaitSuccess(10000);

    Async existsAndDeleteAsync = ctx.async(2);
    adminUtils.topicExists(topicName, ctx.asyncAssertSuccess(res -> existsAndDeleteAsync.countDown()));
    adminUtils.deleteTopic(topicName, ctx.asyncAssertSuccess(res -> existsAndDeleteAsync.countDown()));

    existsAndDeleteAsync.awaitSuccess(10000);
  }

  @Test
  public void testTopicExistsNonExisting(TestContext ctx) throws Exception {
    final String topicName = "testTopicExistsNonExisting";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    Async createAsync = ctx.async();

    AdminUtils adminUtils = AdminUtils.create(vertx, zookeeperHosts, true);

    adminUtils.topicExists(topicName, ctx.asyncAssertSuccess(res -> {
        ctx.assertFalse(res, "Topic must not exist");
        createAsync.complete();
      })
    );
    createAsync.awaitSuccess(10000);
  }

  @Test
  public void testDeleteTopic(TestContext ctx) throws Exception {
    final String topicName = "testDeleteTopic";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    Async async = ctx.async();

    AdminUtils adminUtils = AdminUtils.create(vertx, zookeeperHosts, false);

    adminUtils.createTopic(topicName, 1, 1,
      ctx.asyncAssertSuccess(
        res -> async.complete())
    );

    async.awaitSuccess(10000);

    Async deleteAsync = ctx.async();
    adminUtils.deleteTopic(topicName, ctx.asyncAssertSuccess(res -> deleteAsync.complete()));
    deleteAsync.awaitSuccess(10000);
  }

  @Test
  public void testDeleteNonExistingTopic(TestContext ctx) throws Exception {
    final String topicName = "testDeleteNonExistingTopic";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    AdminUtils adminUtils = AdminUtils.create(vertx, zookeeperHosts, true);

    Async async = ctx.async();

    adminUtils.deleteTopic(topicName, ctx.asyncAssertFailure(res -> {
        ctx.assertEquals("Topic `"+topicName+"` to delete does not exist", res.getLocalizedMessage(),
          "Topic must not exist (not created before)");
        async.complete();
      })
    );
  }

  @Test
  public void testChangeTopicConfig(TestContext ctx) throws Exception {
    final String topicName = "testChangeTopicConfig";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    AdminUtils adminUtils = AdminUtils.create(vertx, zookeeperHosts, false);

    Async createAsync = ctx.async();

    adminUtils.createTopic(topicName, 2, 1,
      ctx.asyncAssertSuccess(
        res -> createAsync.complete()));

    createAsync.awaitSuccess(10000);

    Async changeAsync = ctx.async();
    Map<String, String> properties = new HashMap<>();
    properties.put("delete.retention.ms", "1000");
    properties.put("retention.bytes", "1024");
    adminUtils.changeTopicConfig(topicName, properties,
      ctx.asyncAssertSuccess(res -> changeAsync.complete())
    );

    changeAsync.awaitSuccess(10000);

    Async deleteAsync = ctx.async();
    adminUtils.deleteTopic(topicName, ctx.asyncAssertSuccess(res -> deleteAsync.complete()));
    deleteAsync.awaitSuccess(10000);
  }

  @Test
  public void testChangeTopicConfigWrongConfig(TestContext ctx) throws Exception {
    final String topicName = "testChangeTopicConfigWrongConfig";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    AdminUtils adminUtils = AdminUtils.create(vertx, zookeeperHosts, false);

    Async createAsync = ctx.async();
    adminUtils.createTopic(topicName, 2, 1,
      ctx.asyncAssertSuccess(
        res -> createAsync.complete())
    );

    createAsync.awaitSuccess(10000);

    Async async = ctx.async();
    Map<String, String> properties = new HashMap<>();
    properties.put("this.does.not.exist", "1024L");

    adminUtils.changeTopicConfig(topicName, properties, ctx.asyncAssertFailure(res -> {
        ctx.assertEquals("Unknown topic config name: this.does.not.exist", res.getLocalizedMessage());
        async.complete();
      })
    );
  }

  @Test
  public void testAutoClose(TestContext ctx) throws Exception {
    final String topicName = "testAutoClose";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    AdminUtils adminUtils = AdminUtils.create(vertx, zookeeperHosts, true);

    Async async = ctx.async();

    adminUtils.deleteTopic(topicName, ctx.asyncAssertFailure(deleteResponse -> {
        ctx.assertEquals("Topic `"+topicName+"` to delete does not exist", deleteResponse.getLocalizedMessage(),
          "Topic must not exist (not created before)");
      adminUtils.deleteTopic(topicName, ctx.asyncAssertFailure(secondDeleteResponse -> {
          ctx.assertEquals("ZkClient already closed!", secondDeleteResponse.getLocalizedMessage(),
            "Client must be closed at that point, because autoClose = true");
          async.complete();
        })
      );
      })
    );


  }
}

package io.vertx.kafka.client.tests;

import java.util.Properties;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.client.common.impl.PartitionsForHelper;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerTestBase extends KafkaClusterTestBase {

  private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerTestBase.class);

  private Vertx vertx;
  private KafkaConsumer<String, String> consumer;

  @Before
  public void beforeTest() {
    vertx = Vertx.vertx();
  }

  @After
  public void afterTest(TestContext ctx) {
    close(ctx, consumer.asStream());
    consumer = null;
    vertx.close(ctx.asyncAssertSuccess());
  }

  @Test
  public void testPartitionsForNonExistingTopicWhenAutoTopicCreationSetToFalse(TestContext ctx) throws Exception {
    logger.info("partitionsFor() should return an empty list for non existing topics \"auto.create.topics.enable\" set to \"false\"");

    // Setting auto topic creation to false
    final Properties clusterConfig = new Properties();
    clusterConfig.setProperty("auto.create.topics.enable", "false");
    // Reset and start the cluster with the new configuration
    tearDown();
    startCluster(clusterConfig);

    String topicName = "non_existing_topic";
    String consumerId = topicName;
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumer = KafkaConsumer.create(vertx, config);

    Async done = ctx.async();

    consumer.partitionsFor(topicName, ar -> {
      if (ar.succeeded()) {
        ctx.fail();
      } else {
        ctx.assertEquals(ar.cause().getMessage(), PartitionsForHelper.getNoSuchTopicFailMessageFormat(topicName));
      }
      done.complete();
    });
  }

}

package io.vertx.kafka.client.tests;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SharedProducerShutdownTest extends KafkaClusterTestBase {

  private Vertx vertx;

  @Before
  public void setup() {
    vertx = Vertx.vertx();
  }

  @After
  public void cleanup(TestContext ctx) {
    if (vertx != null) {
      vertx.close(ctx.asyncAssertSuccess());
    }
  }


  public static class TestVerticle extends AbstractVerticle {
    private KafkaProducer<String, String> producer;

    @Override
    public void start() {
      Properties config = new Properties();
      config.putAll(context.config().getMap());
      this.producer = KafkaProducer.createShared(vertx, "the-name", config);
    }

    public Future<Void> sendMessage() {
      return producer.send(KafkaProducerRecord.create("the_topic", UUID.randomUUID().toString(), "the_value"))
        .mapEmpty();
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
      Promise<Void> promise = Promise.promise();
      vertx.setTimer(100, v -> sendMessage().onComplete(promise));
      promise.future().onComplete(stopPromise);
    }
  }

  @Test(timeout = 10000L)
  public void testSharedProducerShutdown(TestContext ctx) {
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    final Async shutdown = ctx.async();

    Async async = ctx.async();
    kafkaCluster.useTo().consumeStrings("the_topic", 1, 3, TimeUnit.SECONDS, async::complete);
    vertx.deployVerticle(TestVerticle.class.getName(), new DeploymentOptions().setInstances(1).setHa(false).setConfig(new JsonObject((Map) config)))
      .onComplete(ctx.asyncAssertSuccess())
      .onComplete(v -> vertx.close().onComplete(ctx.asyncAssertSuccess()).onComplete(x -> {
        vertx = null;
        shutdown.complete();
      }))
      .onComplete(ctx.asyncAssertSuccess());
    shutdown.awaitSuccess(5000);
    async.awaitSuccess(5000);
  }

  @Test(timeout = 3000L)
  public void testVertxOnlyProducer(TestContext ctx) {
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    KafkaProducer<String, String> producer = KafkaProducer.createShared(vertx, "the-name", config);

    Async closeAsync = ctx.async();
    vertx.close().onComplete(v -> closeAsync.complete()).onComplete(ctx.asyncAssertSuccess());
    producer.send(KafkaProducerRecord.create("test", "test"))
      .onComplete(ctx.asyncAssertFailure());
    closeAsync.awaitSuccess(2000);
  }

  @Test(timeout = 10000L)
  public void testMixedUsage(TestContext ctx) {
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    Async async = ctx.async();
    kafkaCluster.useTo().consumeStrings("the_topic", 2, 3, TimeUnit.SECONDS, async::complete);
    final Async shutdown = ctx.async();
    KafkaProducer<String, String> producer = KafkaProducer.createShared(vertx, "the-name", config);
    vertx.deployVerticle(TestVerticle.class.getName(), new DeploymentOptions().setInstances(1).setHa(false).setConfig(new JsonObject((Map) config)))
      .onComplete(ctx.asyncAssertSuccess())
      .onComplete(v -> vertx.close().onComplete(ctx.asyncAssertSuccess()).onComplete(x -> {
        vertx = null;
        shutdown.complete();
      }))
      .onComplete(v -> producer.send(KafkaProducerRecord.create("the_topic", "test")) //should still work since we should not be shutdown yet
        .onComplete(ctx.asyncAssertSuccess()))
      .onComplete(ctx.asyncAssertSuccess());
    shutdown.awaitSuccess(5000);
    async.awaitSuccess(5000);
  }
}

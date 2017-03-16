package io.vertx.kafka.client.tests;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Rx based consumer tests
 */
public class RxConsumerTest extends KafkaClusterTestBase {

  private Vertx vertx;
  private KafkaConsumer<String, String> consumer;

  @Before
  public void beforeTest() {
    vertx = Vertx.vertx();
  }

  @After
  public void afterTest(TestContext ctx) {
    close(ctx, consumer::close);
    consumer = null;
    vertx.close(ctx.asyncAssertSuccess());
  }

  @Test
  public void testConsume(TestContext ctx) throws Exception {
    String topicName = "testConsume";
    Async batch = ctx.async();
    AtomicInteger index = new AtomicInteger();
    int numMessages = 1000;
    kafkaCluster.useTo().produceStrings(numMessages, batch::complete,  () ->
      new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch.awaitSuccess(20000);
    Properties config = kafkaCluster.useTo().getConsumerProperties("testConsume_consumer", "testConsume_consumer", OffsetResetStrategy.EARLIEST);
    Map<String ,String> map = mapConfig(config);
    consumer = KafkaConsumer.create(vertx, map, String.class, String.class);
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger(numMessages);
    consumer.toObservable().subscribe(a -> {
      if (count.decrementAndGet() == 0) {
        done.complete();
      }
    }, ctx::fail);
    consumer.subscribe(Collections.singleton(topicName));
  }
}

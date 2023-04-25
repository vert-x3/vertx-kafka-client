package io.vertx.kafka.client.tests;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(VertxUnitRunner.class)
public class NoClusterTest {

  private Vertx vertx;

  @Before
  public void setUpVertx() {
    vertx = Vertx.vertx();
  }

  @After
  public void tearDownVertx(TestContext ctx) {
    vertx.close()
      .onFailure(ctx::fail)
      .onComplete(ctx.asyncAssertSuccess());
    vertx = null;
  }

  @Test
  public void partitionsForTimeoutMaxBlockMsHigh(TestContext ctx) {
    long maxBlockMs = 2_100; // > the arbitrary hardcoded timeout of 2s
    Properties props = new Properties() {{
      put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
      put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs); // this should trigger the timeout
    }};
    KafkaProducer<String, String> producer = KafkaProducer.create(vertx, props);
    Async async = ctx.async();
    producer.partitionsFor("doesnotexist")
      .onFailure(t -> {
        ctx.assertTrue(t instanceof TimeoutException); // used to fail, since the problem
        async.complete();
      });
  }

  @Test
  public void partitionsForTimeoutMaxBlockMsLow(TestContext ctx) {
    long maxBlockMs = 5; // < the arbitrary hardcoded timeout of 2s -> this test always worked, before the timer got removed
    Properties props = new Properties() {{
      put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
      put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs); // this should trigger the timeout
    }};
    KafkaProducer<String, String> producer = KafkaProducer.create(vertx, props);
    Async async = ctx.async();
    producer.partitionsFor("doesnotexist")
      .onFailure(t -> {
        ctx.assertTrue(t instanceof TimeoutException);
        async.complete();
      });
  }

}

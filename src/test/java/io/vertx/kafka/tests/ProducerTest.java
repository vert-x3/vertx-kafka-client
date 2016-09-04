package io.vertx.kafka.tests;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.KafkaConsumer;
import io.vertx.kafka.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@RunWith(VertxUnitRunner.class)
public class ProducerTest {

  private Vertx vertx;

  @Before
  public void beforeTest() {
    vertx = Vertx.vertx();
  }

  @After
  public void afterTest(TestContext ctx) {
    vertx.close(ctx.asyncAssertSuccess());
  }

  @Test
  public void testProducerDrain(TestContext ctx) {
    MockProducer<String, String> mock = new MockProducer<>(false, new StringSerializer(), new StringSerializer());
    KafkaProducer<String, String> producer = KafkaProducer.create(Vertx.vertx(), mock);
    int sent = 0;
    while (!producer.writeQueueFull()) {
      producer.write(new ProducerRecord<>("the_topic", 0, 0L, "abc", "def"));
      sent++;
    }
    Async async = ctx.async();
    producer.drainHandler(v -> {
      ctx.assertTrue(Context.isOnVertxThread());
      ctx.assertTrue(Context.isOnEventLoopThread());
      async.complete();
    });
    for (int i = 0;i < sent / 2;i++) {
      mock.completeNext();
      assertFalse(producer.writeQueueFull());
    }
    mock.completeNext();
    assertFalse(producer.writeQueueFull());
  }

  @Test
  public void testProducerError(TestContext ctx) {
    MockProducer<String, String> mock = new MockProducer<>(false, new StringSerializer(), new StringSerializer());
    KafkaProducer<String, String> producer = KafkaProducer.create(Vertx.vertx(), mock);
    producer.write(new ProducerRecord<>("the_topic", 0, 0L, "abc", "def"));
    RuntimeException cause = new RuntimeException();
    Async async = ctx.async();
    producer.exceptionHandler(err -> {
      ctx.assertEquals(cause, err);
      async.complete();
    });
    mock.errorNext(cause);
  }

//  @Test
  public void testProducerConsumer(TestContext ctx) throws Exception {

    int numMsg = 100;
    String topic = "abc-def";

    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");;
//    props.put(ProducerConfig.ACKS_CONFIG, "all");
    KafkaProducer<String, String> producer = KafkaProducer.create(vertx, producerProps);
    for (int i = 0;i < numMsg;i++) {
      producer.write(new ProducerRecord<>(topic, 0, 0L, "the_key_" + i, "the_value_" + i));
    }
    producer.close();
//    List<String> msg = ku.readMessages("testtopic", 100);
//    assertEquals(100, msg.size());


    Map<String, Object> consumerProps = new HashMap<>();
    consumerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");;
    consumerProps.put("zookeeper.connect", "localhost:2181");
    consumerProps.put("group.id", "test_group_2");
    consumerProps.put("enable.auto.commit", "false");
    consumerProps.put("auto.offset.reset", "earliest");
    KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consumerProps);
    consumer.subscribe(Collections.singleton(topic));
    AtomicInteger received = new AtomicInteger();

    Async async = ctx.async();
    consumer.handler(rec -> {
      if (received.incrementAndGet() == numMsg) {
        async.complete();
      }
    });
  }
}

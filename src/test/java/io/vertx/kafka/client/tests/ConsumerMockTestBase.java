package io.vertx.kafka.client.tests;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@RunWith(VertxUnitRunner.class)
public abstract class ConsumerMockTestBase {

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
  public void testConsume(TestContext ctx) throws Exception {
    MockConsumer<String, String> mock = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    KafkaReadStream<String, String> consumer = createConsumer(vertx, mock);
    Async doneLatch = ctx.async();
    consumer.handler(record -> {
      ctx.assertEquals("the_topic", record.topic());
      ctx.assertEquals(0, record.partition());
      ctx.assertEquals("abc", record.key());
      ctx.assertEquals("def", record.value());
      consumer.close(v -> doneLatch.complete());
    });
    consumer.subscribe(Collections.singleton("the_topic"), v -> {
      mock.rebalance(Collections.singletonList(new TopicPartition("the_topic", 0)));
      mock.addRecord(new ConsumerRecord<>("the_topic", 0, 0L, "abc", "def"));
      mock.seek(new TopicPartition("the_topic", 0), 0);
    });
  }

  @Test
  public void testBatch(TestContext ctx) throws Exception {
    int num = 50;
    MockConsumer<String, String> mock = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    KafkaReadStream<String, String> consumer = createConsumer(vertx, mock);
    Async doneLatch = ctx.async();
    AtomicInteger count = new AtomicInteger();
    consumer.handler(record -> {
      int val = count.getAndIncrement();
      if (val < num) {
        ctx.assertEquals("the_topic", record.topic());
        ctx.assertEquals(0, record.partition());
        ctx.assertEquals("key-" + val, record.key());
        ctx.assertEquals("value-" + val, record.value());
        if (val == num - 1) {
          consumer.close(v -> doneLatch.complete());
        }
      }
    });
    consumer.subscribe(Collections.singleton("the_topic"), v -> {
      mock.rebalance(Collections.singletonList(new TopicPartition("the_topic", 0)));
      mock.seek(new TopicPartition("the_topic", 0), 0);
      for (int i = 0;i < num;i++) {
        mock.addRecord(new ConsumerRecord<>("the_topic", 0, 0L, "key-" + i, "value-" + i));
      }
    });
  }

  abstract <K, V> KafkaReadStream<K, V> createConsumer(Vertx vertx, Consumer<K, V> consumer);
}

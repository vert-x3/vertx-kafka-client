package io.vertx.kafka.client.tests;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(VertxUnitRunner.class)
public class ExampleTest {

  private static final String TOPIC = "";
  private static final String GROUP = "Test" + System.currentTimeMillis();
  private static final String SERVERS = ""; // bootstrap.servers

  private static final int BATCH_SIZE = 1000;

  private static final Logger logger = LoggerFactory.getLogger(ExampleTest.class);

  private Long prevOffset = null;
  private Vertx vertx;
  private Async async;
  private TestContext context;

  @Test
  public void test(TestContext context) {
    this.vertx = Vertx.vertx();
    this.context = context;
    this.async = context.async();
    Map<String, String> consumerConfig = createConsumerConfig();
    KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consumerConfig);
    TestBatchHandler handler = new TestBatchHandler(BATCH_SIZE, 10000, this::handleAndCheckData, consumer);
    consumer.handler(handler::add);
    consumer.exceptionHandler(e -> logger.error("consumer error", e));
    consumer.subscribe(TOPIC);
  }

  private Future<Void> handleAndCheckData(List<KafkaConsumerRecord> lst) {
    logger.info("got {} values", lst.size());
    for (KafkaConsumerRecord record : lst) {
      long offset = record.offset();
      if (prevOffset != null) {
        long expected = prevOffset + 1;
        if (offset != expected) {
          logger.error("offset {}, expected: {}, diff: {}", offset, expected, offset - expected);
          context.assertEquals(expected, offset);
        }
      }
      prevOffset = offset;
    }
    if (lst.size() < BATCH_SIZE) {
      async.complete();
    }
    return Future.succeededFuture();
  }

  private Map<String, String> createConsumerConfig() {
    Map<String, String> consumerConfig = new HashMap<>();
    consumerConfig.put("bootstrap.servers", SERVERS);
    consumerConfig.put("group.id", GROUP);
    consumerConfig.put("session.timeout.ms", "30000");
    consumerConfig.put("key.deserializer", StringDeserializer.class.getName());
    consumerConfig.put("value.deserializer", StringDeserializer.class.getName());
    consumerConfig.put("auto.offset.reset", "earliest");
    consumerConfig.put("enable.auto.commit", "false");
    return consumerConfig;
  }

  private class TestBatchHandler<T> {

    private ValueHandler<T> valueHandler;
    private KafkaConsumer consumer;
    private ArrayDeque<T> records;
    private int batchSize;
    private long timeout;
    private long timerId;

    public TestBatchHandler(int batchSize, long timeout, ValueHandler<T> valueHandler, KafkaConsumer consumer) {
      this.consumer = consumer;
      this.batchSize = batchSize;
      this.timeout = timeout;
      this.valueHandler = valueHandler;
      this.records = new ArrayDeque<>(batchSize + 1);
      this.timerId = vertx.setTimer(timeout, t -> stopReceiving());
    }

    public void add(T value) {
      synchronized (records) {
        records.add(value);
        if (records.size() >= batchSize) {
          if (vertx.cancelTimer(timerId)) {
            stopReceiving();
          } else {
            consumer.pause();
          }
        }
      }
    }

    private void startReceiving() {
      consumer.resume();
      timerId = vertx.setTimer(timeout, t -> stopReceiving());
    }

    private void stopReceiving() {
      consumer.pause();
      handleBatch().setHandler(h -> startReceiving());
    }

    private Future<Void> handleBatch() {
      synchronized (records) {
        Future<Void> future = Future.future();
        List<T> lst = new ArrayList<>(records);
        valueHandler.handle(lst).setHandler(r -> {
          try {
            synchronized (records) {
              if (records.size() == lst.size()) {
                records.clear();
              } else {
                lst.forEach(records::remove);
              }
            }
          } finally {
            consumer.commit(); // these 2 lines result in lost entries
            future.complete(); //

            // commit().setHandler(future.completer());
          }
        });
        return future;
      }
    }


    /**
     * Using this block results in the mentioned exception,
     * indicating the handler may not be called correctly?
     */
    private Future<Void> commit() {
      Future<Void> f = Future.future();
      consumer.commit(h -> f.complete());
      vertx.setTimer(5000, h -> {
        if (!f.isComplete()) {
          f.complete();
        }
      });
      return f;
    }
  }

  @FunctionalInterface
  interface ValueHandler<T> {
    Future<Void> handle(List<T> values);
  }
}

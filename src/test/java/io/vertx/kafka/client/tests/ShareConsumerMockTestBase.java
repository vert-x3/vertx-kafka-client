package io.vertx.kafka.client.tests;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaShareConsumer;
import io.vertx.kafka.client.consumer.KafkaShareConsumerRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@RunWith(VertxUnitRunner.class)
public abstract class ShareConsumerMockTestBase {

  private Vertx vertx;
  private final String expectedTopic = "topic-1";
  private final int expectedPartition = 0;
  private final String expectedKey = "key-1";
  private final String expectedValue = "value-1";

  @Before
  public void beforeTest() {
    vertx = Vertx.vertx();
  }

  @After
  public void afterTest(TestContext ctx) {
    vertx.close().onComplete(ctx.asyncAssertSuccess());
  }

  abstract <K, V> KafkaShareConsumer<K, V> createShareConsumer(Vertx vertx, ShareConsumer<K, V> shareConsumer);

  private static class CommitErrorShareConsumer<K, V> extends MockShareConsumer<K, V> {
    private final Map<TopicIdPartition, Optional<KafkaException>> commitResult;

    CommitErrorShareConsumer(Map<TopicIdPartition, Optional<KafkaException>> commitResult) {
      this.commitResult = commitResult;
    }

    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync() {
      return commitResult;
    }

    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync(Duration timeout) {
      return commitResult;
    }
  }

  private static class CallbackShareConsumer<K, V> extends MockShareConsumer<K, V> {
    protected AcknowledgementCommitCallback callback;

    @Override
    public void setAcknowledgementCommitCallback(AcknowledgementCommitCallback callback) {
      this.callback = callback;
    }

    @Override
    public void commitAsync() {
      if (callback != null) {
        callback.onComplete(Map.of(), null);
      }
    }
  }

  @Test
  public void testReceiveRecord(TestContext ctx) {
    var mock = new MockShareConsumer<String, String>();
    var shareConsumer = createShareConsumer(vertx, mock);

    Async done = ctx.async();

    shareConsumer.exceptionHandler(ctx::fail);

    shareConsumer.handler(record -> {
      ctx.assertEquals(expectedTopic, record.topic());
      ctx.assertEquals(expectedPartition, record.partition());
      ctx.assertEquals(expectedKey, record.key());
      ctx.assertEquals(expectedValue, record.value());
      shareConsumer.close().onComplete(v -> done.complete());
    });

    shareConsumer.subscribe(expectedTopic)
      .onComplete(ctx.asyncAssertSuccess(v ->
        mock.addRecord(new ConsumerRecord<>(expectedTopic, expectedPartition, 0L, expectedKey, expectedValue))
      ));
  }

  @Test
  public void testAcknowledgeRecord(TestContext ctx) {
    MockShareConsumer<String, String> mock = new MockShareConsumer<>();
    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);

    Async done = ctx.async();

    shareConsumer.exceptionHandler(ctx::fail);

    shareConsumer.handler(record -> shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT)
      .onComplete(ctx.asyncAssertSuccess(v ->
        shareConsumer.close().onComplete(ar -> done.complete())
      )));

    shareConsumer.subscribe(expectedTopic)
      .onComplete(ctx.asyncAssertSuccess(v ->
        mock.addRecord(new ConsumerRecord<>(expectedTopic, expectedPartition, 0L, expectedKey, expectedValue))
      ));
  }

  @Test
  public void testCommitSync(TestContext ctx) {
    MockShareConsumer<String, String> mock = new MockShareConsumer<>();
    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);

    Async done = ctx.async();

    shareConsumer.exceptionHandler(ctx::fail);

    shareConsumer.handler(record -> shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT)
      .compose(v -> shareConsumer.commitSync())
      .onComplete(ctx.asyncAssertSuccess(v ->
        shareConsumer.close().onComplete(ar -> done.complete())
      )));

    shareConsumer.subscribe(expectedTopic)
      .onComplete(ctx.asyncAssertSuccess(v ->
        mock.addRecord(new ConsumerRecord<>(expectedTopic, expectedPartition, 0L, expectedKey, expectedValue))
      ));
  }

  @Test
  public void testCommitSyncWithDuration(TestContext ctx) {
    MockShareConsumer<String, String> mock = new MockShareConsumer<>();
    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);

    Async done = ctx.async();

    shareConsumer.exceptionHandler(ctx::fail);

    shareConsumer.handler(record -> shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT)
      .compose(v -> shareConsumer.commitSync(Duration.ofSeconds(5)))
      .onComplete(ctx.asyncAssertSuccess(map -> {
        ctx.assertTrue(map.isEmpty());
        shareConsumer.close().onComplete(ar -> done.complete());
      })));

    shareConsumer.subscribe(expectedTopic)
      .onComplete(ctx.asyncAssertSuccess(v ->
        mock.addRecord(new ConsumerRecord<>(expectedTopic, expectedPartition, 0L, expectedKey, expectedValue))
      ));
  }

  @Test
  public void testCommitSyncFailsSingleError(TestContext ctx) {
    TopicIdPartition tip = new TopicIdPartition(Uuid.randomUuid(), 0, expectedTopic);
    KafkaException partitionError = new KafkaException("broker rejected");

    CommitErrorShareConsumer<String, String> mock = new CommitErrorShareConsumer<>(
      Map.of(tip, Optional.of(partitionError))
    );

    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);

    Async done = ctx.async();

    shareConsumer.handler(record -> shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT)
      .compose(v -> shareConsumer.commitSync())
      .onComplete(ctx.asyncAssertFailure(ex -> {
        ctx.assertEquals("broker rejected", ex.getMessage());
        shareConsumer.close().onComplete(ar -> done.complete());
      })));

    shareConsumer.subscribe(expectedTopic)
      .onComplete(ctx.asyncAssertSuccess(v ->
        mock.addRecord(new ConsumerRecord<>(expectedTopic, expectedPartition, 0L, expectedKey, expectedValue))
      ));
  }

  @Test
  public void testCommitSyncFailsMultipleErrors(TestContext ctx) {
    TopicIdPartition tip1 = new TopicIdPartition(Uuid.randomUuid(), 0, expectedTopic);
    TopicIdPartition tip2 = new TopicIdPartition(Uuid.randomUuid(), 1, expectedTopic);

    CommitErrorShareConsumer<String, String> mock = new CommitErrorShareConsumer<>(
      Map.of(
        tip1, Optional.of(new KafkaException("error partition 0")),
        tip2, Optional.of(new KafkaException("error partition 1"))
      )
    );

    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);

    Async done = ctx.async();

    shareConsumer.handler(record -> shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT)
      .compose(v -> shareConsumer.commitSync())
      .onComplete(ctx.asyncAssertFailure(ex -> {
        ctx.assertTrue(ex.getMessage().contains("2 partition(s) failed"));
        ctx.assertTrue(ex.getMessage().contains(expectedTopic + "-0"));
        ctx.assertTrue(ex.getMessage().contains(expectedTopic + "-1"));
        shareConsumer.close().onComplete(ar -> done.complete());
      })));

    shareConsumer.subscribe(expectedTopic)
      .onComplete(ctx.asyncAssertSuccess(v ->
        mock.addRecord(new ConsumerRecord<>(expectedTopic, expectedPartition, 0L, expectedKey, expectedValue))
      ));
  }

  @Test
  public void testCommitSyncWithDurationExposesErrors(TestContext ctx) {
    TopicIdPartition tip = new TopicIdPartition(Uuid.randomUuid(), 0, expectedTopic);
    KafkaException partitionError = new KafkaException("broker rejected");

    CommitErrorShareConsumer<String, String> mock = new CommitErrorShareConsumer<>(
      Map.of(tip, Optional.of(partitionError))
    );

    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);

    Async done = ctx.async();

    shareConsumer.handler(record -> shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT)
      .compose(v -> shareConsumer.commitSync(Duration.ofSeconds(5)))
      .onComplete(ctx.asyncAssertSuccess(map -> {
        ctx.assertEquals(1, map.size());
        ctx.assertTrue(map.get(tip).isPresent());
        ctx.assertEquals("broker rejected", map.get(tip).get().getMessage());
        shareConsumer.close().onComplete(ar -> done.complete());
      })));

    shareConsumer.subscribe(expectedTopic)
      .onComplete(ctx.asyncAssertSuccess(v ->
        mock.addRecord(new ConsumerRecord<>(expectedTopic, expectedPartition, 0L, expectedKey, expectedValue))
      ));
  }

  @Test
  public void testUnsubscribe(TestContext ctx) {
    MockShareConsumer<String, String> mock = new MockShareConsumer<>();
    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);

    Async done = ctx.async();

    shareConsumer.exceptionHandler(ctx::fail);

    shareConsumer.subscribe(expectedTopic)
      .compose(v -> shareConsumer.unsubscribe())
      .compose(v -> shareConsumer.subscription())
      .onComplete(ctx.asyncAssertSuccess(topics -> {
        ctx.assertTrue(topics.isEmpty());
        shareConsumer.close().onComplete(ar -> done.complete());
      }));
  }

  @Test
  public void testSubscription(TestContext ctx) {
    MockShareConsumer<String, String> mock = new MockShareConsumer<>();
    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);

    Async done = ctx.async();

    shareConsumer.exceptionHandler(ctx::fail);

    shareConsumer.subscribe(Set.of(expectedTopic, "topic-2"))
      .compose(v -> shareConsumer.subscription())
      .onComplete(ctx.asyncAssertSuccess(topics -> {
        ctx.assertEquals(2, topics.size());
        ctx.assertTrue(topics.contains(expectedTopic));
        ctx.assertTrue(topics.contains("topic-2"));
        shareConsumer.close().onComplete(ar -> done.complete());
      }));
  }

  @Test
  public void testBatchHandler(TestContext ctx) {
    MockShareConsumer<String, String> mock = new MockShareConsumer<>();
    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);

    Async done = ctx.async();

    shareConsumer.exceptionHandler(ctx::fail);

    shareConsumer.batchHandler(records -> {
      ctx.assertEquals(2, records.size());
      ctx.assertEquals(expectedKey, records.recordAt(0).key());
      ctx.assertEquals("key-2", records.recordAt(1).key());
      shareConsumer.close().onComplete(ar -> done.complete());
    });

    shareConsumer.subscribe(expectedTopic)
      .onComplete(ctx.asyncAssertSuccess(v -> {
        mock.addRecord(new ConsumerRecord<>(expectedTopic, expectedPartition, 0L, expectedKey, expectedValue));
        mock.addRecord(new ConsumerRecord<>(expectedTopic, expectedPartition, 1L, "key-2", "value-2"));
      }));
  }

  @Test
  public void testEndHandler(TestContext ctx) {
    MockShareConsumer<String, String> mock = new MockShareConsumer<>();
    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);

    Async done = ctx.async();

    shareConsumer.exceptionHandler(ctx::fail);
    shareConsumer.endHandler(v -> done.complete());

    shareConsumer.subscribe(expectedTopic)
      .compose(v -> shareConsumer.close());
  }

  @Test
  public void testManualPoll(TestContext ctx) {
    MockShareConsumer<String, String> mock = new MockShareConsumer<>();
    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);

    Async done = ctx.async();

    shareConsumer.exceptionHandler(ctx::fail);

    shareConsumer.subscribe(expectedTopic)
      .onComplete(ctx.asyncAssertSuccess(v -> {
        mock.addRecord(new ConsumerRecord<>(expectedTopic, expectedPartition, 0L, expectedKey, expectedValue));
        shareConsumer.poll(Duration.ofSeconds(1))
          .onComplete(ctx.asyncAssertSuccess(records -> {
            ctx.assertEquals(1, records.size());
            KafkaShareConsumerRecord<String, String> record = records.recordAt(0);
            ctx.assertEquals(expectedTopic, record.topic());
            ctx.assertEquals(expectedKey, record.key());
            ctx.assertEquals(expectedValue, record.value());
            shareConsumer.close().onComplete(ar -> done.complete());
          }));
      }));
  }

  @Test
  public void testCommitAsyncCallbackFires(TestContext ctx) {
    CallbackShareConsumer<String, String> mock = new CallbackShareConsumer<>();
    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);

    Async done = ctx.async();

    shareConsumer.setAcknowledgementCommitCallback((offsets, exception) -> {
      ctx.assertNull(exception);
      shareConsumer.close().onComplete(ar -> done.complete());
    });

    shareConsumer.handler(record -> {
      shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
      shareConsumer.commitAsync();
    });

    shareConsumer.subscribe(expectedTopic)
      .onComplete(ctx.asyncAssertSuccess(v ->
        mock.addRecord(new ConsumerRecord<>(expectedTopic, expectedPartition, 0L, expectedKey, expectedValue))
      ));
  }

  @Test
  public void testCommitAsyncCallbackWithError(TestContext ctx) {
    KafkaException commitError = new KafkaException("async commit failed");
    CallbackShareConsumer<String, String> mock = new CallbackShareConsumer<>() {
      @Override
      public void commitAsync() {
        if (callback != null) {
          callback.onComplete(Map.of(), commitError);
        }
      }
    };

    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);
    Async done = ctx.async();

    shareConsumer.setAcknowledgementCommitCallback((offsets, exception) -> {
      ctx.assertNotNull(exception);
      ctx.assertEquals("async commit failed", exception.getMessage());
      shareConsumer.close().onComplete(ar -> done.complete());
    });

    shareConsumer.handler(record -> shareConsumer.commitAsync());

    shareConsumer.subscribe(expectedTopic)
      .onComplete(ctx.asyncAssertSuccess(v ->
        mock.addRecord(new ConsumerRecord<>(expectedTopic, expectedPartition, 0L, expectedKey, expectedValue))
      ));
  }

  @Test
  public void testPollTimeout(TestContext ctx) {
    MockShareConsumer<String, String> mock = new MockShareConsumer<>();
    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);

    Async done = ctx.async();

    shareConsumer.exceptionHandler(ctx::fail);

    shareConsumer.pollTimeout(Duration.ofMillis(500));

    shareConsumer.handler(record -> {
      ctx.assertEquals(expectedValue, record.value());
      shareConsumer.close().onComplete(ar -> done.complete());
    });

    shareConsumer.subscribe(expectedTopic)
      .onComplete(ctx.asyncAssertSuccess(v ->
        mock.addRecord(new ConsumerRecord<>(expectedTopic, expectedPartition, 0L, expectedKey, expectedValue))
      ));
  }

  @Test
  public void testUnwrap(TestContext ctx) {
    MockShareConsumer<String, String> mock = new MockShareConsumer<>();
    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);

    ctx.assertEquals(mock, shareConsumer.unwrap());
  }

  @Test
  public void testPause(TestContext ctx) {
    MockShareConsumer<String, String> mock = new MockShareConsumer<>();
    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);

    Async done = ctx.async();
    shareConsumer.exceptionHandler(ctx::fail);

    shareConsumer.pause();

    shareConsumer.handler(record -> ctx.fail("handler should not be called while paused"));

    shareConsumer.subscribe(expectedTopic)
      .onComplete(ctx.asyncAssertSuccess(v -> {
        mock.addRecord(new ConsumerRecord<>(expectedTopic, expectedPartition, 0L, expectedKey, expectedValue));
        // wait briefly, then verify no record was delivered
        vertx.setTimer(300, t -> shareConsumer.close().onComplete(ar -> done.complete()));
      }));
  }

  @Test
  public void testResumeAfterPause(TestContext ctx) {
    MockShareConsumer<String, String> mock = new MockShareConsumer<>();
    KafkaShareConsumer<String, String> shareConsumer = createShareConsumer(vertx, mock);

    Async done = ctx.async();
    shareConsumer.exceptionHandler(ctx::fail);

    shareConsumer.pause();

    shareConsumer.handler(record -> {
      ctx.assertEquals(expectedValue, record.value());
      shareConsumer.close().onComplete(ar -> done.complete());
    });

    shareConsumer.subscribe(expectedTopic)
      .onComplete(ctx.asyncAssertSuccess(v -> {
        mock.addRecord(new ConsumerRecord<>(expectedTopic, expectedPartition, 0L, expectedKey, expectedValue));
        vertx.setTimer(100, t -> shareConsumer.resume());
      }));
  }

}

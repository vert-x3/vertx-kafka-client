/*
 * Copyright (c) 2011-2019 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package io.vertx.kafka.client.tests;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.client.consumer.AcknowledgeType;
import io.vertx.kafka.client.consumer.KafkaShareConsumer;
import io.vertx.kafka.client.consumer.KafkaShareConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaShareConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class for KafkaShareConsumer integration tests against a real Kafka cluster.
 * <p>
 * All tests here exercise behavior that is common across share consumer implementations.
 * Subclasses only implement {@link #createShareConsumer} to choose the factory under test.
 */
public abstract class ShareConsumerTestBase extends KafkaStrimziTestBase {

  private static final Duration READY_POLL_TIMEOUT = Duration.ofMillis(500);
  private static final long READY_RETRY_DELAY_MS = 250L;
  private static final int READY_MAX_ATTEMPTS = 60;

  private static final String READY_KEY = "__ready_probe";

  protected Vertx vertx;
  protected KafkaShareConsumer<String, String> consumer;

  @Before
  public void beforeTest() {
    vertx = Vertx.vertx();
  }

  @After
  public void afterTest(TestContext ctx) {
    if (consumer != null) {
      Async closeAsync = ctx.async();
      consumer.close().onComplete(ar -> closeAsync.complete());
      closeAsync.awaitSuccess(10000);
      consumer = null;
    }
    vertx.close().onComplete(ctx.asyncAssertSuccess());
  }

  protected Properties shareConsumerProperties(String groupId, String clientId) {
    return shareConsumerProperties(groupId, clientId, false);
  }

  protected Properties shareConsumerProperties(String groupId, String clientId, boolean explicitAck) {
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());
    if (groupId != null) props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    if (clientId != null) {
      props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    }
    if (explicitAck) {
      props.setProperty("share.acknowledgement.mode", "explicit");
    }
    return props;
  }

  protected abstract KafkaShareConsumer<String, String> createShareConsumer(Vertx vertx, Properties config);

  protected Future<Void> awaitShareGroupReady(String topicName, boolean explicitAck) {
    Promise<Void> promise = Promise.promise();
    probe(topicName, explicitAck, promise, 0);
    return promise.future();
  }

  private void probe(String topicName, boolean explicitAck, Promise<Void> promise, int attempt) {
    if (attempt >= READY_MAX_ATTEMPTS) {
      promise.fail("share group did not start reading " + topicName
        + " within " + READY_MAX_ATTEMPTS + " attempts");
      return;
    }

    kafkaCluster.useTo().produceStrings(1, () -> {
    }, () -> new ProducerRecord<>(topicName, READY_KEY, READY_KEY));

    consumer.poll(READY_POLL_TIMEOUT).onComplete(ar -> {
      if (ar.failed()) {
        promise.fail(ar.cause());
      } else if (ar.result().isEmpty()) {
        vertx.setTimer(READY_RETRY_DELAY_MS, t -> probe(topicName, explicitAck, promise, attempt + 1));
      } else {
        acknowledgeAll(ar.result(), explicitAck)
          .onComplete(ackAr -> {
            if (ackAr.failed()) {
              promise.fail(ackAr.cause());
            } else {
              drain(explicitAck, promise);
            }
          });
      }
    });
  }

  private void drain(boolean explicitAck, Promise<Void> promise) {
    consumer.poll(READY_POLL_TIMEOUT).onComplete(ar -> {
      if (ar.failed()) {
        promise.fail(ar.cause());
      } else if (ar.result().isEmpty()) {
        promise.complete();
      } else {
        acknowledgeAll(ar.result(), explicitAck)
          .onComplete(ackAr -> {
            if (ackAr.failed()) {
              promise.fail(ackAr.cause());
            } else {
              drain(explicitAck, promise);
            }
          });
      }
    });
  }

  private Future<Void> acknowledgeAll(KafkaShareConsumerRecords<String, String> records, boolean explicitAck) {
    if (!explicitAck) {
      return Future.succeededFuture();
    }
    Future<Void> chain = Future.succeededFuture();
    for (int i = 0; i < records.size(); i++) {
      KafkaShareConsumerRecord<String, String> record = records.recordAt(i);
      chain = chain.compose(v -> consumer.acknowledge(record, AcknowledgeType.ACCEPT));
    }
    return chain.compose(v -> consumer.commitSync());
  }

  private void produce(String topicName, int count) {
    AtomicInteger index = new AtomicInteger();
    kafkaCluster.useTo().produceStrings(count, () -> {
    }, () -> new ProducerRecord<>(topicName, "key-" + index.get(), "value-" + index.getAndIncrement()));
  }

  @Test
  public void testConsume(TestContext ctx) {
    String topicName = "testShareConsume-" + this.getClass().getName();
    int numMessages = 100;

    kafkaCluster.createTopic(topicName, 1, 1);

    consumer = createShareConsumer(vertx, shareConsumerProperties(topicName, topicName));
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger(numMessages);
    consumer.exceptionHandler(ctx::fail);
    consumer.subscribe(Collections.singleton(topicName))
      .compose(v -> awaitShareGroupReady(topicName, false))
      .onComplete(ar -> {
        ctx.assertTrue(ar.succeeded());
        consumer.handler(rec -> {
          if (count.decrementAndGet() == 0) {
            done.complete();
          }
        });
        produce(topicName, numMessages);
      });
  }

  @Test
  public void testConsumeWithHeaders(TestContext ctx) {
    String topicName = "testShareConsumeWithHeaders-" + this.getClass().getName();
    int numMessages = 50;

    kafkaCluster.createTopic(topicName, 1, 1);

    consumer = createShareConsumer(vertx, shareConsumerProperties(topicName, topicName));
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger(numMessages);
    AtomicInteger headerIndex = new AtomicInteger();
    consumer.exceptionHandler(ctx::fail);
    consumer.subscribe(Collections.singleton(topicName))
      .compose(v -> awaitShareGroupReady(topicName, false))
      .onComplete(ar -> {
        ctx.assertTrue(ar.succeeded());
        consumer.handler(rec -> {
          ctx.assertEquals(1, rec.headers().size());
          ctx.assertEquals("hk-" + headerIndex.get(), rec.headers().get(0).key());
          ctx.assertEquals("hv-" + headerIndex.getAndIncrement(), rec.headers().get(0).value().toString());
          if (count.decrementAndGet() == 0) {
            done.complete();
          }
        });
        AtomicInteger index = new AtomicInteger();
        kafkaCluster.useTo().produceStrings(numMessages, () -> {
        }, () ->
          new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.get(),
            Collections.singletonList(
              new org.apache.kafka.common.header.internals.RecordHeader(
                "hk-" + index.get(), ("hv-" + index.getAndIncrement()).getBytes()))));
      });
  }

  @Test
  public void testPause(TestContext ctx) {
    String topicName = "testSharePause-" + this.getClass().getName();
    int numMessages = 100;

    kafkaCluster.createTopic(topicName, 1, 1);

    consumer = createShareConsumer(vertx, shareConsumerProperties(topicName, topicName));
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger(numMessages);
    AtomicBoolean paused = new AtomicBoolean();
    consumer.exceptionHandler(ctx::fail);
    consumer.subscribe(Collections.singleton(topicName))
      .compose(v -> awaitShareGroupReady(topicName, false))
      .onComplete(ar -> {
        ctx.assertTrue(ar.succeeded());
        consumer.handler(rec -> {
          ctx.assertFalse(paused.get());
          int val = count.decrementAndGet();
          if (val == numMessages / 2) {
            paused.set(true);
            consumer.pause();
            vertx.setTimer(500, id -> {
              paused.set(false);
              consumer.resume();
            });
          }
          if (val == 0) {
            done.complete();
          }
        });
        produce(topicName, numMessages);
      });
  }

  @Test
  public void testFetch(TestContext ctx) {
    String topicName = "testShareFetch-" + this.getClass().getName();
    int numMessages = 100;
    long batchSize = 20L;

    kafkaCluster.createTopic(topicName, 1, 1);

    consumer = createShareConsumer(vertx, shareConsumerProperties(topicName, topicName));
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger(numMessages);
    AtomicLong demand = new AtomicLong();
    consumer.exceptionHandler(ctx::fail);
    consumer.subscribe(Collections.singleton(topicName))
      .compose(v -> awaitShareGroupReady(topicName, false))
      .onComplete(ar -> {
        ctx.assertTrue(ar.succeeded());
        consumer.pause();
        demand.set(batchSize);
        consumer.fetch(batchSize);
        consumer.handler(rec -> {
          long remaining = demand.decrementAndGet();
          ctx.assertTrue(remaining >= 0L);
          if (remaining == 0L) {
            vertx.setTimer(200, id -> {
              demand.set(batchSize);
              consumer.fetch(batchSize);
            });
          }
          if (count.decrementAndGet() == 0) {
            done.complete();
          }
        });
        produce(topicName, numMessages);
      });
  }

  @Test
  public void testSubscription(TestContext ctx) {
    String topicName = "testShareSubscription-" + this.getClass().getName();
    kafkaCluster.createTopic(topicName, 1, 1);

    consumer = createShareConsumer(vertx, shareConsumerProperties(topicName, topicName));
    Async done = ctx.async();
    consumer.exceptionHandler(ctx::fail);
    consumer.subscribe(Collections.singleton(topicName)).onComplete(ar -> {
      ctx.assertTrue(ar.succeeded());
      consumer.subscription().onComplete(subAr -> {
        ctx.assertTrue(subAr.succeeded());
        ctx.assertTrue(subAr.result().contains(topicName));
        done.complete();
      });
    });
  }

  @Test
  public void testBatchHandler(TestContext ctx) {
    String topicName = "testShareBatchHandler-" + this.getClass().getName();
    int numMessages = 50;

    kafkaCluster.createTopic(topicName, 1, 1);

    consumer = createShareConsumer(vertx, shareConsumerProperties(topicName, topicName));
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger();
    consumer.exceptionHandler(ctx::fail);
    consumer.subscribe(Collections.singleton(topicName))
      .compose(v -> awaitShareGroupReady(topicName, false))
      .onComplete(ar -> {
        ctx.assertTrue(ar.succeeded());
        consumer.batchHandler(records -> {
          count.addAndGet(records.size());
          if (count.get() >= numMessages) {
            done.complete();
          }
        });
        produce(topicName, numMessages);
      });
  }

  @Test
  public void testPollTimeout(TestContext ctx) {
    String topicName = "testSharePollTimeout-" + this.getClass().getName();
    kafkaCluster.createTopic(topicName, 1, 1);

    consumer = createShareConsumer(vertx, shareConsumerProperties(topicName, topicName));
    Duration timeout = Duration.ofMillis(500);
    consumer.pollTimeout(timeout);

    Async done = ctx.async();
    consumer.exceptionHandler(ctx::fail);
    consumer.subscribe(Collections.singleton(topicName)).onComplete(ar -> {
      ctx.assertTrue(ar.succeeded());
      long before = System.currentTimeMillis();
      consumer.poll(timeout).onComplete(pollAr -> {
        ctx.assertTrue(pollAr.succeeded());
        ctx.assertTrue(pollAr.result().isEmpty());
        long elapsed = System.currentTimeMillis() - before;
        ctx.assertTrue(elapsed >= timeout.toMillis(),
          "poll() must block at least as long as the timeout, got " + elapsed + "ms");
        done.complete();
      });
    });
  }

  @Test
  public void testConsumeWithPoll(TestContext ctx) {
    String topicName = "testShareConsumeWithPoll-" + this.getClass().getName();
    int numMessages = 50;

    kafkaCluster.createTopic(topicName, 1, 1);

    consumer = createShareConsumer(vertx, shareConsumerProperties(topicName, topicName));
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger(numMessages);
    consumer.exceptionHandler(ctx::fail);
    consumer.subscribe(Collections.singleton(topicName))
      .compose(v -> awaitShareGroupReady(topicName, false))
      .onComplete(ar -> {
        ctx.assertTrue(ar.succeeded());
        AtomicLong timerId = new AtomicLong();
        timerId.set(vertx.setPeriodic(1000, t ->
          consumer.poll(Duration.ofMillis(500))
            .onComplete(pollAr -> {
              if (pollAr.succeeded()) {
                if (count.addAndGet(-pollAr.result().size()) <= 0) {
                  vertx.cancelTimer(timerId.get());
                  done.complete();
                }
              } else {
                ctx.fail(pollAr.cause());
              }
            })));
        produce(topicName, numMessages);
      });
  }

  @Test
  public void testConsumeWithPollNoMessages(TestContext ctx) {
    String topicName = "testShareConsumeWithPollNoMessages-" + this.getClass().getName();
    kafkaCluster.createTopic(topicName, 1, 1);

    consumer = createShareConsumer(vertx, shareConsumerProperties(topicName, topicName));
    Async done = ctx.async();
    AtomicInteger emptyPolls = new AtomicInteger(3);
    consumer.exceptionHandler(ctx::fail);
    consumer.subscribe(Collections.singleton(topicName)).onComplete(ar -> {
      ctx.assertTrue(ar.succeeded());
      vertx.setPeriodic(500, t -> {
        consumer.poll(Duration.ofMillis(100)).onComplete(pollAr -> {
          if (pollAr.succeeded()) {
            ctx.assertTrue(pollAr.result().isEmpty(), "expected no records on empty topic");
            if (emptyPolls.decrementAndGet() == 0) {
              vertx.cancelTimer(t);
              done.complete();
            }
          } else {
            ctx.fail(pollAr.cause());
          }
        });
      });
    });
  }

  @Test
  public void testPollNoSubscribe(TestContext ctx) {
    consumer = createShareConsumer(vertx, shareConsumerProperties("testSharePollNoSub", null));
    Async done = ctx.async();
    consumer.poll(Duration.ofMillis(100)).onComplete(ar -> {
      ctx.assertTrue(ar.failed());
      ctx.assertTrue(ar.cause() instanceof IllegalStateException);
      done.complete();
    });
  }

  @Test
  public void testAcknowledgeAndCommitSync(TestContext ctx) {
    String topicName = "testShareAckCommitSync-" + this.getClass().getName();
    int numMessages = 10;

    kafkaCluster.createTopic(topicName, 1, 1);

    consumer = createShareConsumer(vertx, shareConsumerProperties(topicName, topicName, true));
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger(numMessages);
    consumer.exceptionHandler(ctx::fail);
    consumer.subscribe(Collections.singleton(topicName))
      .compose(v -> awaitShareGroupReady(topicName, true))
      .onComplete(ar -> {
        ctx.assertTrue(ar.succeeded());
        consumer.handler(rec ->
          consumer.acknowledge(rec, AcknowledgeType.ACCEPT).onComplete(ackAr -> {
            ctx.assertTrue(ackAr.succeeded());
            consumer.commitSync().onComplete(commitAr -> {
              ctx.assertTrue(commitAr.succeeded());
              if (count.decrementAndGet() == 0) {
                done.complete();
              }
            });
          }));
        produce(topicName, numMessages);
      });
  }

  @Test
  public void testAcknowledgeRelease(TestContext ctx) {
    String topicName = "testShareAckRelease-" + this.getClass().getName();

    kafkaCluster.createTopic(topicName, 1, 1);

    consumer = createShareConsumer(vertx, shareConsumerProperties(topicName, topicName, true));
    Async done = ctx.async();
    AtomicInteger deliveries = new AtomicInteger();
    consumer.exceptionHandler(ctx::fail);
    consumer.subscribe(Collections.singleton(topicName))
      .compose(v -> awaitShareGroupReady(topicName, true))
      .onComplete(ar -> {
        ctx.assertTrue(ar.succeeded());
        consumer.handler(rec -> {
          deliveries.incrementAndGet();
          if (rec.deliveryCount() <= 1) {
            consumer.acknowledge(rec, AcknowledgeType.RELEASE)
              .compose(v -> consumer.commitSync())
              .onComplete(relAr -> ctx.assertTrue(relAr.succeeded()));
          } else {
            consumer.acknowledge(rec, AcknowledgeType.ACCEPT)
              .compose(v -> consumer.commitSync())
              .onComplete(accAr -> {
                ctx.assertTrue(accAr.succeeded());
                ctx.assertTrue(deliveries.get() >= 2, "record should have been redelivered");
                done.complete();
              });
          }
        });
        kafkaCluster.useTo().produceStrings(1, () -> {
        }, () -> new ProducerRecord<>(topicName, "key", "value"));
      });
  }

  @Test
  public void testCommitAsync(TestContext ctx) {
    String topicName = "testShareCommitAsync-" + this.getClass().getName();
    int numMessages = 10;

    kafkaCluster.createTopic(topicName, 1, 1);

    consumer = createShareConsumer(vertx, shareConsumerProperties(topicName, topicName, true));
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger(numMessages);
    consumer.exceptionHandler(ctx::fail);
    consumer.subscribe(Collections.singleton(topicName))
      .compose(v -> awaitShareGroupReady(topicName, true))
      .onComplete(ar -> {
        ctx.assertTrue(ar.succeeded());
        consumer.setAcknowledgementCommitCallback((offsets, exception) -> {
          ctx.assertNull(exception);
          int acked = offsets.values().stream().mapToInt(Set::size).sum();
          if (count.addAndGet(-acked) <= 0) done.complete();
        });
        consumer.handler(rec -> {
          consumer.acknowledge(rec, AcknowledgeType.ACCEPT);
          consumer.commitAsync();
        });
        produce(topicName, numMessages);
      });
  }

  @Test(expected = org.apache.kafka.common.KafkaException.class)
  public void testMissingGroupIdFailsFast(TestContext ctx) {
    Properties config = shareConsumerProperties(null, null);
    config.remove(ConsumerConfig.GROUP_ID_CONFIG);
    consumer = createShareConsumer(vertx, config);
  }
}

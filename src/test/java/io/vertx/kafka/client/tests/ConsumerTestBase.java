/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vertx.kafka.client.tests;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerImpl;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Base class for consumer tests
 */
public abstract class ConsumerTestBase extends KafkaClusterTestBase {

  private Vertx vertx;
  private KafkaReadStream<String, String> consumer;
  private KafkaReadStream<String, String> consumer2;

  @Before
  public void beforeTest() {
    vertx = Vertx.vertx();
  }

  @After
  public void afterTest(TestContext ctx) {
    close(ctx, consumer);
    close(ctx, consumer2);
    consumer = null;
    consumer2 = null;
    vertx.close(ctx.asyncAssertSuccess());
  }

  @Test
  public void testConsume(TestContext ctx) throws Exception {
    final String topicName = "testConsume";
    String consumerId = topicName;
    Async batch = ctx.async();
    AtomicInteger index = new AtomicInteger();
    int numMessages = 1000;
    kafkaCluster.useTo().produceStrings(numMessages, batch::complete, () ->
      new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch.awaitSuccess(20000);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumer = createConsumer(vertx, config);
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger(numMessages);
    consumer.exceptionHandler(ctx::fail);
    consumer.handler(rec -> {
      if (count.decrementAndGet() == 0) {
        done.complete();
      }
    });
    consumer.subscribe(Collections.singleton(topicName));
  }

  @Test
  public void testConsumePattern(TestContext ctx) throws Exception {
    final String topicName1 = "testConsumePattern1";
    final String topicName2 = "testConsumePattern2";
    String consumerId = topicName1 + "-" + topicName2;
    Async batch = ctx.async(2);
    AtomicInteger index = new AtomicInteger();
    int numMessages = 500;
    kafkaCluster.useTo().produceStrings(numMessages, batch::countDown, () ->
      new ProducerRecord<>(topicName1, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    kafkaCluster.useTo().produceStrings(numMessages, batch::countDown, () ->
      new ProducerRecord<>(topicName2, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch.awaitSuccess(20000);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumer = createConsumer(vertx, config);
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger(numMessages * 2);
    consumer.exceptionHandler(ctx::fail);
    consumer.handler(rec -> {
      if (count.decrementAndGet() == 0) {
        done.complete();
      }
    });
    Pattern pattern = Pattern.compile("testConsumePattern\\d");
    consumer.subscribe(pattern);
  }

  @Test
  public void testStreamWithHeader(TestContext ctx) {
    int numMessages = 1000;
    String topicName = "testStreamWithHeader";
    Properties config = setupConsumeWithHeaders(ctx, numMessages, topicName);
    consumer = createConsumer(vertx, config);
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger(numMessages);
    AtomicInteger headerIndex = new AtomicInteger();
    consumer.exceptionHandler(ctx::fail);
    consumer.handler(rec -> {
      Header[] headers = rec.headers().toArray();
      ctx.assertEquals(1, headers.length);
      Header header = headers[0];
      ctx.assertEquals("header_key" + headerIndex.get(), header.key());
      ctx.assertEquals("header_value" + headerIndex.getAndIncrement(), new String(header.value()));
      if (count.decrementAndGet() == 0) {
        done.complete();
      }
    });
    consumer.subscribe(Collections.singleton(topicName));
  }

  @Test
  public void testConsumerWithHeader(TestContext ctx) {
    int numMessages = 1000;
    String topicName = "testConsumerWithHeader";
    Properties config = setupConsumeWithHeaders(ctx, numMessages, topicName);
    consumer = createConsumer(vertx, config);
    KafkaConsumer<String, String> consumer = new KafkaConsumerImpl<>(this.consumer);
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger(numMessages);
    AtomicInteger headerIndex = new AtomicInteger();
    consumer.exceptionHandler(ctx::fail);
    consumer.handler(rec -> {
      List<KafkaHeader> headers = rec.headers();
      ctx.assertEquals(1, headers.size());
      KafkaHeader header = headers.get(0);
      ctx.assertEquals("header_key" + headerIndex.get(), header.key());
      ctx.assertEquals("header_value" + headerIndex.getAndIncrement(), header.value().toString());
      if (count.decrementAndGet() == 0) {
        done.complete();
      }
    });
    consumer.subscribe(Collections.singleton(topicName));
  }

  private Properties setupConsumeWithHeaders(TestContext ctx, int numMessages, String topicName) {
    Async batch = ctx.async();
    AtomicInteger index = new AtomicInteger();
    kafkaCluster.useTo().produceStrings(numMessages, batch::complete, () ->
      new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.get(),
        Collections.singletonList(new RecordHeader("header_key" + index.get(), ("header_value" + index.getAndIncrement()).getBytes()))));
    batch.awaitSuccess(20000);
    Properties config = kafkaCluster.useTo().getConsumerProperties(topicName, topicName, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    return config;
  }

  @Test
  public void testPause(TestContext ctx) throws Exception {
    final String topicName = "testPause";
    final String consumerId = topicName;

    Async batch = ctx.async();
    AtomicInteger index = new AtomicInteger();
    int numMessages = 1000;
    kafkaCluster.useTo().produceStrings(numMessages, batch::complete, () ->
      new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch.awaitSuccess(20000);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumer = createConsumer(vertx, config);
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger(numMessages);
    consumer.exceptionHandler(ctx::fail);
    AtomicBoolean paused = new AtomicBoolean();
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
    consumer.subscribe(Collections.singleton(topicName));
  }

  @Test
  public void testFetch(TestContext ctx) throws Exception {
    final String topicName = "testPause";
    final String consumerId = topicName;

    Async batch = ctx.async();
    AtomicInteger index = new AtomicInteger();
    int numMessages = 1000;
    kafkaCluster.useTo().produceStrings(numMessages, batch::complete, () ->
      new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch.awaitSuccess(20000);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumer = createConsumer(vertx, config);
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger(numMessages);
    consumer.exceptionHandler(ctx::fail);
    AtomicLong demand = new AtomicLong();
    long batchSize = 200L;
    consumer.handler(rec -> {
      long remaining = demand.decrementAndGet();
      ctx.assertTrue(remaining >= 0L);
      if (remaining == 0L) {
        vertx.setTimer(500, id -> {
          demand.set(batchSize);
          consumer.fetch(batchSize);
        });
      }
      int val = count.decrementAndGet();
      if (val == 0) {
        done.complete();
      }
    });

    consumer.pause();
    demand.set(batchSize);
    consumer.fetch(batchSize);

    consumer.subscribe(Collections.singleton(topicName));
  }

  @Test
  public void testPauseSingleTopic(TestContext ctx) throws Exception {
    final String topicName = "testPauseSingleTopic";
    final String consumerId = topicName;

    Async batch = ctx.async();
    AtomicInteger index = new AtomicInteger();
    int numMessages = 5_000;
    kafkaCluster.useTo().produceStrings(numMessages, batch::complete, () ->
      new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch.awaitSuccess(20_000);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumer = createConsumer(vertx, config);
    Async done = ctx.async();

    TopicPartition partition = new TopicPartition(topicName, 0);

    AtomicInteger count = new AtomicInteger(numMessages);
    consumer.exceptionHandler(ctx::fail);
    AtomicBoolean paused = new AtomicBoolean();
    consumer.batchHandler(recs -> {
      ctx.assertFalse(paused.get());
    });
    consumer.handler(rec -> {
      int val = count.decrementAndGet();
      if (val == numMessages / 3) {
        paused.set(true);
        consumer.pause(Collections.singleton(partition));
        vertx.setTimer(500, id -> {
          paused.set(false);
          consumer.resume(Collections.singleton(partition));
        });
      }
      if (val == 0) {
        done.complete();
      }
    });
    consumer.assign(Collections.singleton(partition));
  }

  @Test
  public void testCommit(TestContext ctx) throws Exception {
    String topicName = "testCommit";
    String consumerId = topicName;
    Async batch1 = ctx.async();
    AtomicInteger index = new AtomicInteger();
    int numMessages = 500;
    kafkaCluster.useTo().produceStrings(numMessages, batch1::complete, () ->
      new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch1.awaitSuccess(10000);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumer = createConsumer(vertx, config);
    consumer.exceptionHandler(ctx::fail);
    Async committed = ctx.async();
    AtomicInteger count = new AtomicInteger();
    consumer.handler(rec -> {
      int idx = count.getAndIncrement();
      ctx.assertEquals("key-" + idx, rec.key());
      ctx.assertEquals("value-" + idx, rec.value());
      if (idx == numMessages - 1) {
        consumer.commit(ctx.asyncAssertSuccess(v1 -> {
          consumer.close(v2 -> {
            committed.complete();
          });
        }));
      }
    });
    //consumer.subscribe(Collections.singleton(topicName));
    // Using assign instead of subscribe makes the test _much_ faster (2,5 seconds vs 10,5 seconds)
    consumer.assign(Collections.singleton(new TopicPartition(topicName, 0)));
    committed.awaitSuccess(10000);
    Async batch2 = ctx.async();
    kafkaCluster.useTo().produceStrings(numMessages, batch2::complete, () ->
      new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch2.awaitSuccess(10000);
    consumer = createConsumer(vertx, config);
    consumer.exceptionHandler(ctx::fail);
    Async done = ctx.async();
    consumer.handler(rec -> {
      int idx = count.getAndIncrement();
      ctx.assertEquals("key-" + idx, rec.key());
      ctx.assertEquals("value-" + idx, rec.value());
      if (idx == numMessages * 2 - 1) {
        consumer.commit(ctx.asyncAssertSuccess(v1 -> {
          done.complete();
        }));
      }
    });
    consumer.subscribe(Collections.singleton(topicName));
  }

  @Test
  public void testCommitWithOffsets(TestContext ctx) throws Exception {
    String topicName = "testCommitWithOffsets";
    String consumerId = topicName;
    Async batch1 = ctx.async();
    AtomicInteger index = new AtomicInteger();
    int numMessages = 500;
    kafkaCluster.useTo().produceStrings(numMessages, batch1::complete, () ->
      new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch1.awaitSuccess(10000);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumer = createConsumer(vertx, config);
    consumer.exceptionHandler(ctx::fail);
    Async committed = ctx.async(2);
    AtomicInteger count = new AtomicInteger();
    consumer.handler(rec -> {
      int val = count.incrementAndGet();
      switch (val) {
        case 101:
          TopicPartition the_topic = new TopicPartition(topicName, 0);
          consumer.commit(Collections.singletonMap(the_topic, new OffsetAndMetadata(rec.offset())),
            ctx.asyncAssertSuccess(v -> committed.countDown()));
          break;
        case 500:
          committed.countDown();
          break;
      }
    });
    // consumer.subscribe(Collections.singleton(topicName));
    // Assign is much faster than subscribe
    consumer.assign(Collections.singleton(new TopicPartition(topicName, 0)));

    committed.awaitSuccess(10000);
    Async closed = ctx.async();
    consumer.close(v -> closed.complete());
    closed.awaitSuccess(10000);
    consumer = createConsumer(vertx, config);
    count.set(100);
    Async done = ctx.async();
    consumer.handler(rec -> {
      int idx = count.getAndIncrement();
      ctx.assertEquals("key-" + idx, rec.key());
      ctx.assertEquals("value-" + idx, rec.value());
      if (idx == numMessages - 1) {
        done.complete();
      }
    });
    consumer.subscribe(Collections.singleton(topicName));
  }

  @Test
  public void testCommitAfterPoll(TestContext ctx) throws Exception {

    String topicName = "testCommitAfterPoll";
    String consumerId = topicName;
    Async batch = ctx.async();
    AtomicInteger index = new AtomicInteger();
    int numMessages = 10;
    kafkaCluster.useTo().produceStrings(numMessages, batch::complete, () ->
      new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch.awaitSuccess(10000);

    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumer = createConsumer(vertx, config);
    consumer.exceptionHandler(ctx::fail);

    Async subscribe = ctx.async();
    consumer.subscribe(Collections.singleton(topicName), ar1 -> {
      subscribe.complete();
    });
    subscribe.await();

    Async consume = ctx.async();
    consumer.poll(Duration.ofSeconds(10), rec -> {
      if (rec.result().count() == 10) {
        consume.countDown();
      }
    });
    consume.await();

    Async committed = ctx.async();
    TopicPartition the_topic = new TopicPartition(topicName, 0);
    consumer.commit(Collections.singletonMap(the_topic, new OffsetAndMetadata(10)), ar2 -> {
      committed.complete();
    });
    committed.await();
  }

  @Test
  public void testRebalance(TestContext ctx) throws Exception {
    String topicName = "testRebalance";
    String consumerId = topicName;
    kafkaCluster.createTopic(topicName, 2, 1);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    Context context = vertx.getOrCreateContext();
    consumer = createConsumer(context, config);

    // We need to rename the client to avoid javax.management.InstanceAlreadyExistsException
    // see https://github.com/vert-x3/vertx-kafka-client/issues/5
    config.setProperty("client.id", "the_consumer2");
    consumer2 = createConsumer(vertx, config);
    consumer.handler(rec -> {});
    consumer2.handler(rec -> {});
    Async rebalanced = ctx.async(2);
    AtomicInteger status = new AtomicInteger();
    consumer.partitionsAssignedHandler(partitions -> {
      ctx.assertEquals(Vertx.currentContext(), context);
      switch (status.getAndIncrement()) {
        case 0:
          consumer2.subscribe(Collections.singleton(topicName));
          ctx.assertEquals(2, partitions.size());
          ctx.assertTrue(partitions.contains(new TopicPartition(topicName, 0)));
          ctx.assertTrue(partitions.contains(new TopicPartition(topicName, 1)));
          break;
        case 1:
          ctx.fail();
          break;
        case 2:
          ctx.assertEquals(1, partitions.size());
          rebalanced.countDown();
          break;
      }
    });
    consumer.partitionsRevokedHandler(partitions -> {
      ctx.assertEquals(Vertx.currentContext(), context);
      switch (status.getAndIncrement()) {
        case 0:
          ctx.fail();
          break;
        case 1:
          ctx.assertEquals(2, partitions.size());
          ctx.assertTrue(partitions.contains(new TopicPartition(topicName, 0)));
          ctx.assertTrue(partitions.contains(new TopicPartition(topicName, 1)));
          break;
        case 2:
          ctx.fail();
          break;
        case 3:
          ctx.assertEquals(1, partitions.size());
          break;
      }
    });
    AtomicInteger status2 = new AtomicInteger();
    consumer2.partitionsAssignedHandler(partitions -> {
      switch (status2.getAndIncrement()) {
        case 0:
          ctx.assertEquals(1, partitions.size());
          rebalanced.countDown();
          break;
      }
    });
    consumer.subscribe(Collections.singleton(topicName));
  }

  @Test
  public void testSeek(TestContext ctx) throws Exception {
    int numMessages = 500;
    testSeek("the_topic_0", numMessages, ctx, () -> {
      consumer.seek(new TopicPartition("the_topic_0", 0), 0);
    }, -numMessages);
  }

  @Test
  public void testSeekToBeginning(TestContext ctx) throws Exception {
    int numMessages = 500;
    testSeek("the_topic_1", numMessages, ctx, () -> {
      consumer.seekToBeginning(Collections.singleton(new TopicPartition("the_topic_1", 0)));
    }, -numMessages);
  }

  @Test
  public void testSeekToEnd(TestContext ctx) throws Exception {
    int numMessages = 500;
    testSeek("the_topic_2", numMessages, ctx, () -> {
      consumer.seekToEnd(Collections.singleton(new TopicPartition("the_topic_2", 0)));
    }, 0);
  }

  private void testSeek(String topic, int numMessages, TestContext ctx, Runnable seeker, int abc) throws Exception {
    kafkaCluster.createTopic(topic, 1, 1);
    String consumerId = topic;
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    Context context = vertx.getOrCreateContext();
    consumer = createConsumer(context, config);
    Async batch1 = ctx.async();
    AtomicInteger index = new AtomicInteger();
    kafkaCluster.useTo().produceStrings(numMessages, batch1::complete, () ->
      new ProducerRecord<>(topic, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch1.awaitSuccess(10000);
    AtomicInteger count = new AtomicInteger(numMessages);
    Async done = ctx.async();
    consumer.handler(record -> {
      int dec = count.decrementAndGet();
      if (dec >= 0) {
        ctx.assertEquals("key-" + (numMessages - dec - 1), record.key());
      } else {
        ctx.assertEquals("key-" + (-1 - dec), record.key());
      }
      if (dec == 0) {
        seeker.run();
      }
      if (dec == abc) {
        done.complete();
      }
    });
    consumer.subscribe(Collections.singleton(topic));
  }

  @Test
  public void testSeekAfterConsume(TestContext ctx) throws Exception {
    String topic = "testSeekAfterConsume";
    kafkaCluster.createTopic(topic, 1, 1);

    Properties config = kafkaCluster.useTo().getConsumerProperties(topic, topic, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    Context context = vertx.getOrCreateContext();
    consumer = createConsumer(context, config);
    Async batch1 = ctx.async();
    AtomicInteger index = new AtomicInteger();
    kafkaCluster.useTo().produceStrings(5000, batch1::complete, () ->
      new ProducerRecord<>(topic, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch1.awaitSuccess(10000);
    Async done = ctx.async();

    TopicPartition topicPartition = new TopicPartition(topic, 0);
    List<String> keys = new ArrayList<>();
    consumer.assign(Collections.singleton(topicPartition), assignRes -> {
      // We set a handler => consumer starts polling
      AtomicBoolean seek = new AtomicBoolean();
      consumer.handler(record -> {
        if (seek.compareAndSet(false, true)) {
          ctx.assertEquals("key-0", record.key());
          // Need to pause the consumer as it's currently delivering a batch of 10 elements
          consumer.pause();
          // Seek to offset 0
          consumer.seekToBeginning(Collections.singleton(topicPartition), res -> {
            consumer.position(topicPartition, ctx.asyncAssertSuccess(posRes -> {
              ctx.assertEquals(0L, posRes, "Expecting offset 0 after seek to 0");
              consumer.resume();
            }));
          });
        } else {
          keys.add(record.key());
          if (keys.size() == 5000) {
            for (int i = 0; i < 5000; i++) {
              ctx.assertEquals("key-" + i, keys.get(i));
            }
            done.complete();
          }
        }
      });
    });
  }

  @Test
  public void testSubscription(TestContext ctx) throws Exception {
    String topicName = "testSubscription";
    String consumerId = topicName;
    kafkaCluster.createTopic(topicName, 1, 1);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    Context context = vertx.getOrCreateContext();
    consumer = createConsumer(context, config);

    Async done = ctx.async();

    consumer.handler(record -> {
      // no need for handling incoming records in this test
    });

    consumer.subscribe(Collections.singleton(topicName), asyncResult -> {

      if (asyncResult.succeeded()) {

        consumer.subscription(asyncResult1 -> {

          if (asyncResult1.succeeded()) {

            ctx.assertTrue(asyncResult1.result().contains(topicName));
            done.complete();

          } else {
            ctx.fail();
          }
        });

      } else {
        ctx.fail();
      }

    });
  }

  @Test
  public void testAssign(TestContext ctx) throws Exception {
    String topicName = "testAssign";
    String consumerId = topicName;
    kafkaCluster.createTopic(topicName, 1, 1);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    Context context = vertx.getOrCreateContext();
    consumer = createConsumer(context, config);

    Async done = ctx.async();

    consumer.handler(record -> {
      // no need for handling incoming records in this test
    });

    TopicPartition partition = new TopicPartition(topicName, 0);

    consumer.assign(Collections.singleton(partition), asyncResult -> {

      if (asyncResult.succeeded()) {

        consumer.assignment(asyncResult1 -> {

          if (asyncResult1.succeeded()) {

            ctx.assertTrue(asyncResult1.result().contains(partition));
            done.complete();

          } else {
            ctx.fail();
          }
        });

      } else {
        ctx.fail();
      }

    });
  }

  @Test
  public void testSetHandlerThenAssign(TestContext ctx) throws Exception {
    String topicName = "testSetHandlerThenAssign";
    String consumerId = topicName;
    kafkaCluster.createTopic(topicName, 1, 1);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    int numMessages = 1;
    Async finished = ctx.async(numMessages + 2);
    kafkaCluster.useTo().produceStrings(numMessages, finished::countDown, () ->
      new ProducerRecord<>(topicName, 0, "key", "value"));
    Context context = vertx.getOrCreateContext();
    consumer = createConsumer(context, config);

    Async assigned = ctx.async();
    Async handler = ctx.async();

    consumer.handler(record -> {
      ctx.assertTrue(handler.isCompleted() && assigned.isCompleted());
      finished.countDown();
    });
    handler.complete();

    consumer.batchHandler(records -> {
      ctx.assertTrue(handler.isCompleted() && assigned.isCompleted());
      finished.countDown();
    });

    TopicPartition partition = new TopicPartition(topicName, 0);

    consumer.assign(Collections.singleton(partition), asyncResult -> {

      if (asyncResult.succeeded()) {

        assigned.complete();

      } else {
        ctx.fail();
      }

    });
  }

  @Test
  public void testAssignThenSetHandler(TestContext ctx) throws Exception {
    String topicName = "testAssignThenSetHandler";
    String consumerId = topicName;
    kafkaCluster.createTopic(topicName, 1, 1);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId,
      OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    int numMessages = 1;
    Async finished = ctx.async(numMessages + 2);
    kafkaCluster.useTo().produceStrings(numMessages, finished::countDown,
      () -> new ProducerRecord<>(topicName, 0, "key", "value"));
    Context context = vertx.getOrCreateContext();
    consumer = createConsumer(context, config);

    Async assigned = ctx.async();
    Async handler = ctx.async();

    consumer.batchHandler(records -> {
      ctx.assertTrue(handler.isCompleted() && assigned.isCompleted());
      finished.countDown();
    });

    TopicPartition partition = new TopicPartition(topicName, 0);

    consumer.assign(Collections.singleton(partition), asyncResult -> {

      if (asyncResult.succeeded()) {

        assigned.complete();

      } else {
        ctx.fail();
      }

    });

    consumer.handler(record -> {
      ctx.assertTrue(handler.isCompleted() && assigned.isCompleted());
      finished.countDown();
    });
    handler.complete();

  }

  @Test
  public void testAssignAndSeek(TestContext ctx) throws Exception {
    String topicName = "testAssignAndSeek";
    String consumerId = topicName;
    kafkaCluster.createTopic(topicName, 1, 1);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    int numMessages = 5_000;
    Async finished = ctx.async(numMessages);
    AtomicInteger index = new AtomicInteger();
    kafkaCluster.useTo().produceStrings(numMessages, finished::complete, () ->
      new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    finished.await();
    Context context = vertx.getOrCreateContext();
    consumer = createConsumer(context, config);

    TopicPartition partition = new TopicPartition(topicName, 0);
    consumer.assign(Collections.singleton(partition), asyncResult -> {
      ctx.assertTrue(asyncResult.succeeded());
    });

    //state: 0=1st time, 1=seek called, 2=seek executed, 3=offset 0 for 2nd time
    AtomicInteger state = new AtomicInteger(0);
    Async async = ctx.async();
    consumer.batchHandler(records -> {
      switch (state.get()) {
        case 0: // seek not yet called
        case 1: // seek called but not completed
          break;
        case 2: // seek completed
          for (ConsumerRecord record : records) {
            long offset = record.offset();
            if (offset > 5
              && !async.isCompleted()) {
              ctx.fail();
            }
            if (record.offset() == 0) {
              async.complete();
            }
          }
          break;
      }

    });
    consumer.handler(record -> {
      long offset = record.offset();
      switch (state.get()) {
        case 0: // seek not yet called
        case 1: // seek called but not completed
          if (offset == 5) {
            state.set(1);
            consumer.seek(partition, 0, ar -> {
              state.set(2);
              ctx.assertTrue(ar.succeeded());
            });
          }
          break;
      }

    });

  }

  @Test
  public void testReassign(TestContext ctx) throws Exception {
    String topicName1 = "testReassign1";
    String topicName2 = "testReassign2";
    String consumerId = "testReassign";
    kafkaCluster.createTopic(topicName1, 1, 1);
    kafkaCluster.createTopic(topicName2, 1, 1);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    // two topics, each with 5000 messages
    int numMessages = 5_000;
    Async finished = ctx.async(2);
    AtomicInteger index = new AtomicInteger();
    kafkaCluster.useTo().produceStrings(numMessages, finished::countDown, () ->
      new ProducerRecord<>(topicName1, 0, "1key-" + index.get(), "1value-" + index.getAndIncrement()));
    kafkaCluster.useTo().produceStrings(numMessages, finished::countDown, () ->
      new ProducerRecord<>(topicName2, 0, "2key-" + index.get(), "2value-" + index.getAndIncrement()));
    finished.await();

    Context context = vertx.getOrCreateContext();
    consumer = createConsumer(context, config);

    TopicPartition partition1 = new TopicPartition(topicName1, 0);
    TopicPartition partition2 = new TopicPartition(topicName2, 0);
    consumer.assign(Collections.singleton(partition1), asyncResult -> {
      ctx.assertTrue(asyncResult.succeeded());
    });

    //state: 0=1st time, 1=seek called, 2=seek executed, 3=offset 0 for 2nd time
    AtomicInteger state = new AtomicInteger(0);
    Async async = ctx.async();
    consumer.batchHandler(records -> {
      switch (state.get()) {
        case 0://initial assignment, not started reassignment
        case 1://initial assignment, started reassignment, but not done yet
          break;
        case 2:
          for (ConsumerRecord record : records) {
            long offset = record.offset();
            if (record.topic().equals(topicName1)) {
              ctx.fail("Seen a " + topicName1 + " message after reassignment");
            }
            if (offset == numMessages - 1) {
              async.complete();
            }
          }
      }
    });

    consumer.handler(record -> {
      long offset = record.offset();
      switch (state.get()) {
        case 0://initial assignment, not started reassignment
        case 1://initial assignment, started reassignment, but not done yet
          if (record.topic().equals(topicName2)) {
            ctx.fail("Seen a " + topicName2 + " message before reassignment");
          }
          if (offset == 5) {
            state.set(1);
            consumer.assign(Collections.singleton(partition2), ar -> {
              state.set(2);
              ctx.assertTrue(ar.succeeded());
            });
          }
          break;
      }
    });

  }

  @Test
  public void testListTopics(TestContext ctx) throws Exception {
    String topicName = "testListTopics";
    String consumerId = topicName;
    kafkaCluster.createTopic(topicName, 1, 1);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    Context context = vertx.getOrCreateContext();
    consumer = createConsumer(context, config);

    Async done = ctx.async();

    consumer.handler(record -> {
      // no need for handling incoming records in this test
    });

    consumer.subscribe(Collections.singleton(topicName), asyncResult -> {

      if (asyncResult.succeeded()) {

        consumer.listTopics(asyncResult1 -> {

          if (asyncResult1.succeeded()) {

            ctx.assertTrue(asyncResult1.result().containsKey(topicName));
            done.complete();

          } else {
            ctx.fail();
          }
        });

      } else {
        ctx.fail();
      }

    });
  }

  @Test
  public void testPartitionsFor(TestContext ctx) throws Exception {
    String topicName = "testPartitionsFor";
    String consumerId = topicName;
    kafkaCluster.createTopic(topicName, 2, 1);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    Context context = vertx.getOrCreateContext();
    consumer = createConsumer(context, config);

    Async done = ctx.async();

    consumer.partitionsFor(topicName, ar -> {
      if (ar.succeeded()) {
        List<PartitionInfo> partitionInfo = ar.result();
        ctx.assertEquals(2, partitionInfo.size());
      } else {
        ctx.fail();
      }
      done.complete();
    });
  }

  @Test
  public void testPositionEmptyTopic(TestContext ctx) throws Exception {
    String topicName = "testPositionEmptyTopic";
    String consumerId = topicName;
    kafkaCluster.createTopic(topicName, 1, 1);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    Context context = vertx.getOrCreateContext();
    consumer = createConsumer(context, config);

    Async done = ctx.async();

    consumer.handler(record -> {
      // no need for handling incoming records in this test
    });

    consumer.subscribe(Collections.singleton(topicName), asyncResult -> {

      if (asyncResult.succeeded()) {
        consumer.partitionsFor(topicName, asyncResult1 -> {
          if (asyncResult.succeeded()) {
            for (org.apache.kafka.common.PartitionInfo pi : asyncResult1.result()) {
              TopicPartition tp = new TopicPartition(topicName, pi.partition());
              consumer.position(tp, asyncResult2 -> {
                if (asyncResult2.succeeded()) {
                  ctx.assertTrue(asyncResult2.result() == 0);
                  done.complete();
                } else {
                  ctx.fail();
                }
              });
            }
          } else {
            ctx.fail();
          }
        });

      } else {
        ctx.fail();
      }

    });
  }

  @Test
  public void testPositionNonEmptyTopic(TestContext ctx) throws Exception {
    String topicName = "testPositionNonEmptyTopic";
    String consumerId = topicName;
    kafkaCluster.createTopic(topicName, 1, 1);
    Async batch = ctx.async();
    AtomicInteger index = new AtomicInteger();
    int numMessages = 1000;
    kafkaCluster.useTo().produceStrings(numMessages, batch::complete, ()
      -> new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch.awaitSuccess(20000);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    Context context = vertx.getOrCreateContext();
    consumer = createConsumer(context, config);

    Async done = ctx.async();

    AtomicInteger count = new AtomicInteger(numMessages);
    consumer.exceptionHandler(ctx::fail);
    consumer.handler(rec -> {
      if (count.decrementAndGet() == 0) {
        consumer.partitionsFor(topicName, asyncResult -> {
          if (asyncResult.succeeded()) {
            for (org.apache.kafka.common.PartitionInfo pi : asyncResult.result()) {
              TopicPartition tp = new TopicPartition(topicName, pi.partition());
              consumer.position(tp, asyncResult1 -> {
                if (asyncResult1.succeeded()) {
                  ctx.assertTrue(asyncResult1.result() == numMessages);
                  done.complete();
                } else {
                  ctx.fail();
                }
              });
            }
          } else {
            ctx.fail();
          }
        });
      }
    });
    consumer.subscribe(Collections.singleton(topicName));
  }



  /*
    Tests beginningOffset
   */
  @Test
  public void testBeginningOffset(TestContext ctx) throws Exception {
    testBeginningEndOffset(ctx, true);
  }

  /*
    Tests endOffset (boolean parameter = false)
   */
  @Test
  public void testEndOffset(TestContext ctx) throws Exception {
    testBeginningEndOffset(ctx, false);
  }

  /*
   Tests test beginningOffset or endOffset, depending on beginningOffset = true or false
  */
  public void testBeginningEndOffset(TestContext ctx, boolean beginningOffset) throws Exception {
    String topicName = "testBeginningEndOffset_" + (beginningOffset ? "beginning" : "end");
    String consumerId = topicName;
    Async batch = ctx.async();
    AtomicInteger index = new AtomicInteger();
    int numMessages = 1000;
    kafkaCluster.useTo().produceStrings(numMessages, batch::complete, () ->
      new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch.awaitSuccess(20000);

    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    Context context = vertx.getOrCreateContext();
    consumer = createConsumer(context, config);

    consumer.exceptionHandler(ctx::fail);

    Set<TopicPartition> topicPartitions = new HashSet<>();
    TopicPartition topicPartition = new TopicPartition(topicName, 0);
    topicPartitions.add(topicPartition);

    // Test contains two sub-tests
    Async done = ctx.async(2);
    consumer.handler(handler -> {
      // nothing to do in this test
    });

    consumer.subscribe(Collections.singleton(topicName), ctx.asyncAssertSuccess(subscribeRes -> {
        if (beginningOffset) {
          consumer.beginningOffsets(topicPartitions, beginningOffsetResult -> {
            ctx.assertTrue(beginningOffsetResult.succeeded());
            // expect one result
            ctx.assertEquals(1, beginningOffsetResult.result().size());
            // beginning offset must be 0
            ctx.assertEquals(0L, beginningOffsetResult.result().get(topicPartition));
            done.countDown();
          });
          consumer.beginningOffsets(topicPartition, beginningOffsetResult -> {
            ctx.assertTrue(beginningOffsetResult.succeeded());
            // beginning offset must be 0
            ctx.assertEquals(0L, beginningOffsetResult.result());
            done.countDown();
          });
        }
        // Tests for endOffset
        else {
          consumer.endOffsets(topicPartitions, endOffsetResult -> {
            ctx.assertTrue(endOffsetResult.succeeded());
            ctx.assertEquals(1, endOffsetResult.result().size());
            // endOffset must be equal to the number of ingested messages
            ctx.assertEquals((long) numMessages, endOffsetResult.result().get(topicPartition));
            done.countDown();
          });

          consumer.endOffsets(topicPartition, endOffsetResult -> {
            ctx.assertTrue(endOffsetResult.succeeded());
            // endOffset must be equal to the number of ingested messages
            ctx.assertEquals((long) numMessages, endOffsetResult.result());
            done.countDown();
          });
        }
      }
    ));
  }


  @Test
  public void testOffsetsForTimes(TestContext ctx) throws Exception {
    String topicName = "testOffsetsForTimes";
    String consumerId = topicName;
    Async batch = ctx.async();
    AtomicInteger index = new AtomicInteger();
    int numMessages = 1000;
    long beforeProduce = System.currentTimeMillis();
    kafkaCluster.useTo().produceStrings(numMessages, batch::complete, () ->
      new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch.awaitSuccess(20000);
    long produceDuration = System.currentTimeMillis() - beforeProduce;
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    Context context = vertx.getOrCreateContext();
    consumer = createConsumer(context, config);

    consumer.exceptionHandler(ctx::fail);

    TopicPartition topicPartition = new TopicPartition(topicName, 0);

    // Test contains two sub-tests
    Async done = ctx.async(2);
    consumer.handler(handler -> {
      // nothing to do in this test
    });

    consumer.subscribe(Collections.singleton(topicName), ctx.asyncAssertSuccess(subscribeRes -> {
      // search by timestamp
      // take timestamp BEFORE start of ingestion and add half of the ingestion duration to it
      long searchTimestamp = beforeProduce + (produceDuration / 2);
      consumer.offsetsForTimes(Collections.singletonMap(topicPartition, searchTimestamp), ctx.asyncAssertSuccess(offsetAndTimestamps -> {
        OffsetAndTimestamp offsetAndTimestamp = offsetAndTimestamps.get(topicPartition);
        ctx.assertEquals(1, offsetAndTimestamps.size());
        // Offset must be somewhere between beginningOffset and endOffset
        ctx.assertTrue(offsetAndTimestamp.offset() >= 0L && offsetAndTimestamp.offset() <= (long) numMessages,
          "Invalid offset 0 <= " + offsetAndTimestamp.offset() + " <= " + numMessages);
        // Timestamp of returned offset must be at >= searchTimestamp
        ctx.assertTrue(offsetAndTimestamp.timestamp() >= searchTimestamp);
        done.countDown();
      }));

      consumer.offsetsForTimes(topicPartition, searchTimestamp, ctx.asyncAssertSuccess(offsetAndTimestamp -> {
        // Offset must be somewhere between beginningOffset and endOffset
        ctx.assertTrue(offsetAndTimestamp.offset() >= 0L && offsetAndTimestamp.offset() <= (long) numMessages,
          "Invalid offset 0 <= " + offsetAndTimestamp.offset() + " <= " + numMessages);
        // Timestamp of returned offset must be at >= searchTimestamp
        ctx.assertTrue(offsetAndTimestamp.timestamp() >= searchTimestamp);
        done.countDown();
      }));
    }));
  }

  @Test
  // Test uses KafkaConsumer instead of KafkaReadStream to test the full API
  public void testOffsetsForTimesWithTimestampInFuture(TestContext ctx) throws Exception {
    String topicName = "testOffsetsForTimesWithTimestampInFuture";
    String consumerId = topicName;
    Async batch = ctx.async();
    AtomicInteger index = new AtomicInteger();
    int numMessages = 10;
    kafkaCluster.useTo().produceStrings(numMessages, batch::complete, () ->
      new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch.awaitSuccess(20000);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    KafkaConsumer<Object, Object> wrappedConsumer = KafkaConsumer.create(vertx, config);
    wrappedConsumer.exceptionHandler(ctx::fail);

    io.vertx.kafka.client.common.TopicPartition topicPartition = new io.vertx.kafka.client.common.TopicPartition(topicName, 0);

    Async done = ctx.async(2);
    done.handler(r -> wrappedConsumer.close());
    wrappedConsumer.handler(handler -> {
      // nothing to do in this test
    });

    // Note offsetsForTimes doesn't require a subscription or assignment
    // search by timestamp
    // take a timestamp in the future, such that no offset exists
    long searchTimestamp = System.currentTimeMillis();
    wrappedConsumer.offsetsForTimes(topicPartition, searchTimestamp, ctx.asyncAssertSuccess(offsetAndTimestamp -> {
      ctx.assertEquals(null, offsetAndTimestamp, "Must return null because no offset for a timestamp in the future can exist");
      done.countDown();
    }));

    wrappedConsumer.offsetsForTimes(Collections.singletonMap(topicPartition, searchTimestamp), ctx.asyncAssertSuccess(offsetAndTimestamps -> {
      io.vertx.kafka.client.consumer.OffsetAndTimestamp offsetAndTimestamp = offsetAndTimestamps.get(topicPartition);
      ctx.assertEquals(0, offsetAndTimestamps.size(), "Must not return a result, because no Offset is found");
      ctx.assertEquals(null, offsetAndTimestamp, "Must return null because no offset for a timestamp in the future can exist");
      done.countDown();
    }));
  }

  @Test
  public void testBatchHandler(TestContext ctx) throws Exception {
    String topicName = "testBatchHandler";
    String consumerId = topicName;
    Async batch1 = ctx.async();
    AtomicInteger index = new AtomicInteger();
    int numMessages = 500;
    kafkaCluster.useTo().produceStrings(numMessages, batch1::complete, () ->
      new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch1.awaitSuccess(10000);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    Context context = vertx.getOrCreateContext();
    consumer = createConsumer(context, config);
    Async batchHandler = ctx.async();
    consumer.batchHandler(records -> {
      ctx.assertEquals(numMessages, records.count());
      batchHandler.complete();
    });
    consumer.exceptionHandler(ctx::fail);
    consumer.handler(rec -> {});
    consumer.subscribe(Collections.singleton(topicName));
  }

  @Test
  public void testConsumerBatchHandler(TestContext ctx) throws Exception {
    String topicName = "testConsumerBatchHandler";
    String consumerId = topicName;
    Async batch1 = ctx.async();
    AtomicInteger index = new AtomicInteger();
    int numMessages = 500;
    kafkaCluster.useTo().produceStrings(numMessages, batch1::complete, () ->
      new ProducerRecord<>(topicName, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
    batch1.awaitSuccess(10000);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    KafkaConsumer<Object, Object> wrappedConsumer = KafkaConsumer.create(vertx, config);
    wrappedConsumer.exceptionHandler(ctx::fail);
    AtomicInteger count = new AtomicInteger(numMessages);
    Async batchHandler = ctx.async();
    batchHandler.handler(ar -> wrappedConsumer.close());
    wrappedConsumer.batchHandler(records -> {
      ctx.assertEquals(numMessages, records.size());
      for (int i = 0; i < records.size(); i++) {
        KafkaConsumerRecord<Object, Object> record = records.recordAt(i);
        int dec = count.decrementAndGet();
        if (dec >= 0) {
          ctx.assertEquals("key-" + (numMessages - dec - 1), record.key());
        } else {
          ctx.assertEquals("key-" + (-1 - dec), record.key());
        }
      }
      batchHandler.complete();
    });
    wrappedConsumer.handler(rec -> {});
    wrappedConsumer.subscribe(Collections.singleton(topicName));
  }

  @Test
  public void testPollExceptionHandler(TestContext ctx) throws Exception {
    Properties config = kafkaCluster.useTo().getConsumerProperties("someRandomGroup", "someRandomClientID", OffsetResetStrategy.EARLIEST);
    config.remove("group.id");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumer = createConsumer(vertx, config);
    Async done = ctx.async();
    consumer.exceptionHandler(ex -> {
      ctx.assertTrue(ex instanceof InvalidGroupIdException);
      done.complete();
    });
    consumer.handler(System.out::println).subscribe(Collections.singleton("someTopic"));
  }

  @Test
  public void testPollTimeout(TestContext ctx) throws Exception {
    Async async = ctx.async();
    String topicName = "testPollTimeout";
    Properties config = kafkaCluster.useTo().getConsumerProperties(topicName, topicName, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    io.vertx.kafka.client.common.TopicPartition topicPartition = new io.vertx.kafka.client.common.TopicPartition(topicName, 0);
    KafkaConsumer<Object, Object> consumerWithCustomTimeout = KafkaConsumer.create(vertx, config);

    int pollingTimeout = 1500;
    // Set the polling timeout to 1500 ms (default is 1000)
    consumerWithCustomTimeout.pollTimeout(Duration.ofMillis(pollingTimeout));
    // Subscribe to the empty topic (we want the poll() call to timeout!)
    consumerWithCustomTimeout.subscribe(topicName, subscribeRes -> {
      consumerWithCustomTimeout.handler(rec -> {}); // Consumer will now immediately poll once
      long beforeSeek = System.currentTimeMillis();
      consumerWithCustomTimeout.seekToBeginning(topicPartition, seekRes -> {
        long durationWShortTimeout = System.currentTimeMillis() - beforeSeek;
        ctx.assertTrue(durationWShortTimeout >= pollingTimeout, "Operation must take at least as long as the polling timeout");
        consumerWithCustomTimeout.close();
        async.countDown();
      });
    });
  }

  @Test
  public void testNotCommitted(TestContext ctx) throws Exception {

    String topicName = "testNotCommitted";
    String consumerId = topicName;
    kafkaCluster.createTopic(topicName, 1, 1);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    Async done = ctx.async();

    KafkaConsumer<Object, Object> consumer = KafkaConsumer.create(vertx, config);
    consumer.handler(rec -> {});
    consumer.partitionsAssignedHandler(partitions -> {
      for (io.vertx.kafka.client.common.TopicPartition partition : partitions) {
        consumer.committed(partition, ar -> {
          if (ar.succeeded()) {
            ctx.assertNull(ar.result());
          } else {
            ctx.fail(ar.cause());
          }
        });
      }
      done.complete();
    });

    consumer.subscribe(Collections.singleton(topicName));
  }

  @Test
  public void testConsumeWithPoll(TestContext ctx) {
    final String topicName = "testConsumeWithPoll";
    final String consumerId = topicName;
    Async batch = ctx.async();
    int numMessages = 1000;
    kafkaCluster.useTo().produceStrings(numMessages, batch::complete, () ->
      new ProducerRecord<>(topicName, "value")
    );
    batch.awaitSuccess(20000);
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger(numMessages);

    consumer.subscribe(Collections.singleton(topicName), subscribeResult -> {

      if (subscribeResult.succeeded()) {

        vertx.setPeriodic(1000, t -> {
          consumer.poll(Duration.ofMillis(100), pollResult -> {
            if (pollResult.succeeded()) {
              if (count.updateAndGet(o -> count.get() - pollResult.result().size()) == 0) {
                vertx.cancelTimer(t);
                done.complete();
              }
            } else {
              ctx.fail();
            }
          });
        });

      } else {
        ctx.fail();
      }
    });
  }

  @Test
  public void testConsumeWithPollNoMessages(TestContext ctx) {
    final String topicName = "testConsumeWithPollNoMessages";
    final String consumerId = topicName;
    Properties config = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);
    Async done = ctx.async();
    AtomicInteger count = new AtomicInteger(5);

    consumer.subscribe(Collections.singleton(topicName), subscribeResult -> {

      if (subscribeResult.succeeded()) {

        vertx.setPeriodic(1000, t -> {
          consumer.poll(Duration.ofMillis(100), pollResult -> {
            if (pollResult.succeeded()) {
              if (pollResult.result().size() > 0) {
                ctx.fail();
              } else {
                if (count.decrementAndGet() == 0) {
                  vertx.cancelTimer(t);
                  done.complete();
                }
              }
            } else {
              ctx.fail();
            }
          });
        });

      } else {
        ctx.fail();
      }
    });
  }

  <K, V> KafkaReadStream<K, V> createConsumer(Context context, Properties config) throws Exception {
    CompletableFuture<KafkaReadStream<K, V>> ret = new CompletableFuture<>();
    context.runOnContext(v -> {
      try {
        ret.complete(createConsumer(context.owner(), config));
      } catch (Exception e) {
        ret.completeExceptionally(e);
      }
    });
    return ret.get(10, TimeUnit.SECONDS);
  }

  abstract <K, V> KafkaReadStream<K, V> createConsumer(Vertx vertx, Properties config);
}

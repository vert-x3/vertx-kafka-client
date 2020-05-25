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

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.producer.KafkaWriteStream;

/**
 * Transactional Producer tests
 */
public class TransactionalProducerTest extends KafkaClusterTestBase {

  private Vertx vertx;
  private KafkaWriteStream<String, String> producer;

  @BeforeClass
  public static void setUp() throws IOException {
    // Override to use 3 broker setup
    kafkaCluster = kafkaCluster().deleteDataPriorToStartup(true).addBrokers(3).startup();
  }

  @Before
  public void beforeTest() {
    vertx = Vertx.vertx();
  }

  @After
  public void afterTest(TestContext ctx) {
    close(ctx, producer);
    vertx.close(ctx.asyncAssertSuccess());
  }

  @Before
  public void init(TestContext ctx) {
    final Properties config = kafkaCluster.useTo().getProducerProperties("testTransactional_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "producer-1");
    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    config.put(ProducerConfig.ACKS_CONFIG, "all");

    producer = producer(Vertx.vertx(), config);
    producer.exceptionHandler(ctx::fail);
  }

  @Test
  public void producedRecordsAreSeenAfterTheyHaveBeenCommitted(TestContext ctx) {
    final String topicName = "transactionalProduce";
    int numMessages = 1000;

    final Async done = ctx.async();
    final AtomicInteger seq = new AtomicInteger();
    final KafkaReadStream<String, String> consumer = consumer(topicName);
    consumer.exceptionHandler(ctx::fail);
    consumer.handler(record -> {
      int count = seq.getAndIncrement();
      ctx.assertEquals("key-" + count, record.key());
      ctx.assertEquals("value-" + count, record.value());
      ctx.assertEquals("header_value-" + count, new String(record.headers().headers("header_key").iterator().next().value()));
      if (count == numMessages) {
        done.complete();
      }
    });
    consumer.subscribe(Collections.singleton(topicName));

    producer.initTransactions(ctx.asyncAssertSuccess());
    producer.beginTransaction(ctx.asyncAssertSuccess());
    for (int i = 0; i <= numMessages; i++) {
      final ProducerRecord<String, String> record = createRecord(topicName, i);
      producer.write(record, ctx.asyncAssertSuccess());
    }
    producer.commitTransaction(ctx.asyncAssertSuccess());
  }

  @Test
  public void abortTransactionKeepsTopicEmpty(TestContext ctx) {
    final String topicName = "transactionalProduceAbort";
    final Async done = ctx.async();

    producer.initTransactions(ctx.asyncAssertSuccess());
    producer.beginTransaction(ctx.asyncAssertSuccess());
    final ProducerRecord<String, String> record_0 = createRecord(topicName, 0);
    producer.write(record_0, whenWritten -> {
      producer.abortTransaction(ctx.asyncAssertSuccess());
      final KafkaReadStream<String, String> consumer = consumer(topicName);
      consumer.exceptionHandler(ctx::fail);
      consumer.subscribe(Collections.singleton(topicName));
      consumer.poll(Duration.ofSeconds(5), records -> {
        ctx.assertTrue(records.result().isEmpty());
        done.complete();
      });
    });
  }

  @Test
  public void transactionHandlingFailsIfInitWasNotCalled(TestContext ctx) {
    producer.beginTransaction(ctx.asyncAssertFailure(cause -> {
      ctx.assertTrue(cause instanceof KafkaException);
    }));
    producer.commitTransaction(ctx.asyncAssertFailure(cause -> {
      ctx.assertTrue(cause instanceof KafkaException);
    }));
    producer.abortTransaction(ctx.asyncAssertFailure(cause -> {
      ctx.assertTrue(cause instanceof KafkaException);
    }));
  }

  @Test
  public void initTransactionsFailsOnWrongConfig(TestContext ctx) {
    final Properties noTransactionalIdConfigured = kafkaCluster.useTo().getProducerProperties("nonTransactionalProducer");
    noTransactionalIdConfigured.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    noTransactionalIdConfigured.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    final KafkaWriteStream<Object, Object> nonTransactionalProducer = producer(Vertx.vertx(), noTransactionalIdConfigured);
    nonTransactionalProducer.exceptionHandler(ctx::fail);
    nonTransactionalProducer.initTransactions(ctx.asyncAssertFailure(cause -> {
      ctx.assertTrue(cause instanceof IllegalStateException);
    }));
  }

  private <K, V> KafkaReadStream<K, V> consumer(final String topicName) {
    final Properties config = kafkaCluster.useTo().getConsumerProperties("group-" + topicName, "consumer-" + topicName, OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    return KafkaReadStream.create(vertx, config);
  }

  private ProducerRecord<String, String> createRecord(final String topicName, final int i) {
    final ProducerRecord<String, String> record = new ProducerRecord<>(topicName, 0, "key-" + i, "value-" + i);
    record.headers().add("header_key", ("header_value-" + i).getBytes());
    return record;
  }

}

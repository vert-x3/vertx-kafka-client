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
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.KafkaWriteStream;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;

/**
 * Tests using mock producers
 */
@RunWith(VertxUnitRunner.class)
public class ProducerMockTest {

  private static class TestProducer extends MockProducer<String, String> {
    public TestProducer() {
      super(false, new StringSerializer(), new StringSerializer());
    }

    public void assertCompleteNext() {
      while (!completeNext()) {
        Thread.yield();
      }
    }

    public void assertErrorNext(RuntimeException e) {
      while (!errorNext(e)) {
        Thread.yield();
      }
    }
  }

  private static class SimulatedWriteException extends Exception {

  }
  /*
    Simulates an error during write -- invokes the callback with a RuntimeException
   */
  private static class TestProducerWriteError extends MockProducer<String, String> {
    public TestProducerWriteError() {
      super(false, new StringSerializer(), new StringSerializer());
    }

    @Override
    public synchronized Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
      callback.onCompletion(null, new SimulatedWriteException());
      return super.send(record, callback);
    }
  }



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
  public void testProducerDrain(TestContext ctx) throws Exception {
    testProducerDrain(ctx, null);
  }

  @Test
  public void testProducerFailureDrain(TestContext ctx) throws Exception {
    testProducerDrain(ctx, new RuntimeException());
  }

  private void testProducerDrain(TestContext ctx, RuntimeException failure) throws Exception {
    TestProducer mock = new TestProducer();
    KafkaWriteStream<String, String> producer = ProducerTest.producer(Vertx.vertx(), mock);
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
      if (failure != null) {
        mock.assertErrorNext(failure);
      } else {
        mock.assertCompleteNext();
      }
    }
    if (failure != null) {
      mock.assertErrorNext(failure);
    } else {
      mock.assertCompleteNext();
    }
    assertFalse(producer.writeQueueFull());
  }

  @Test
  public void testProducerError(TestContext ctx) throws Exception {
    TestProducer mock = new TestProducer();
    KafkaWriteStream<String, String> producer = ProducerTest.producer(Vertx.vertx(), mock);
    producer.write(new ProducerRecord<>("the_topic", 0, 0L, "abc", "def"));
    RuntimeException cause = new RuntimeException();
    Async async = ctx.async();
    producer.exceptionHandler(err -> {
      ctx.assertEquals(cause, err);
      async.complete();
    });
    mock.assertErrorNext(cause);
  }

//  @Test
  public void testProducerConsumer(TestContext ctx) throws Exception {

    int numMsg = 100;
    String topic = "abc-def";

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//    props.put(ProducerConfig.ACKS_CONFIG, "all");
    KafkaWriteStream<String, String> producer = ProducerTest.producer(vertx, producerProps);
    for (int i = 0;i < numMsg;i++) {
      producer.write(new ProducerRecord<>(topic, 0, 0L, "the_key_" + i, "the_value_" + i));
    }
    producer.close();
//    List<String> msg = ku.readMessages("testtopic", 100);
//    assertEquals(100, msg.size());


    Map<String, Object> consumerProps = new HashMap<>();
    consumerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    consumerProps.put("zookeeper.connect", "localhost:2181");
    consumerProps.put("group.id", "test_group_2");
    consumerProps.put("enable.auto.commit", "false");
    consumerProps.put("auto.offset.reset", "earliest");
    KafkaReadStream<String, String> consumer = KafkaReadStream.create(vertx, consumerProps);
    consumer.subscribe(Collections.singleton(topic));
    AtomicInteger received = new AtomicInteger();

    Async async = ctx.async();
    consumer.handler(rec -> {
      if (received.incrementAndGet() == numMsg) {
        async.complete();
      }
    });
  }

  @Test
  public void testWriteWithSimulatedError(TestContext ctx) {
    TestProducerWriteError mock = new TestProducerWriteError();
    KafkaProducer<String, String> prod = KafkaProducer.create(vertx, mock);
    KafkaProducerRecord<String, String> record = KafkaProducerRecord.create("myTopic", "test");
    vertx.exceptionHandler(h -> {
      if(!(h instanceof SimulatedWriteException)) {
        ctx.fail(h);
      }
    });
    prod.write(record);
  }
}

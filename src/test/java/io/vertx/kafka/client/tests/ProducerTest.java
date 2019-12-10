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

import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.producer.impl.KafkaProducerImpl;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Producer tests
 */
public class ProducerTest extends KafkaClusterTestBase {

  private Vertx vertx;
  private KafkaWriteStream<String, String> producer;

  @Before
  public void beforeTest() {
    vertx = Vertx.vertx();
  }

  @After
  public void afterTest(TestContext ctx) {
    close(ctx, producer);
    vertx.close(ctx.asyncAssertSuccess());
  }

  @Test
  public void testStreamProduce(TestContext ctx) throws Exception {
    String topicName = "testStreamProduce";
    Properties config = kafkaCluster.useTo().getProducerProperties("testStreamProduce_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producer = producer(Vertx.vertx(), config);
    producer.exceptionHandler(ctx::fail);
    int numMessages = 100000;
    for (int i = 0;i < numMessages;i++) {
      ProducerRecord<String, String> record = new ProducerRecord<>(topicName, 0, "key-" + i, "value-" + i);
      record.headers().add("header_key", ("header_value-" + i).getBytes());
      producer.write(record);
    }
    assertReceiveMessages(ctx, topicName, numMessages);
  }

  @Test
  public void testProducerProduce(TestContext ctx) throws Exception {
    String topicName = "testProducerProduce";
    Properties config = kafkaCluster.useTo().getProducerProperties("testProducerProduce_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producer = producer(Vertx.vertx(), config);
    producer.exceptionHandler(ctx::fail);
    KafkaProducer<String, String> producer = new KafkaProducerImpl<>(this.vertx, this.producer);
    int numMessages = 100000;
    for (int i = 0;i < numMessages;i++) {
      producer.write(KafkaProducerRecord.create(topicName, "key-" + i, "value-" + i, 0)
        .addHeader("header_key", "header_value-" + i));
    }
    assertReceiveMessages(ctx, topicName, numMessages);
  }

  private void assertReceiveMessages(TestContext ctx, String topicName, int numMessages) {
    Async done = ctx.async();
    AtomicInteger seq = new AtomicInteger();
    kafkaCluster.useTo().consumeStrings(() -> seq.get() < numMessages, done::complete, Collections.singleton(topicName), record -> {
      int count = seq.getAndIncrement();
      ctx.assertEquals("key-" + count, record.key());
      ctx.assertEquals("value-" + count, record.value());
      ctx.assertEquals("header_value-" + count, new String(record.headers().headers("header_key").iterator().next().value()));
    });
  }

  @Test
  public void testBlockingBroker(TestContext ctx) throws Exception {
    // Use a port different from default 9092, because Broker IS running
    int port = 9091;
    Async serverAsync = ctx.async();
    vertx.createNetServer().connectHandler(so -> {
    }).listen(port, ctx.asyncAssertSuccess(v -> serverAsync.complete()));
    serverAsync.awaitSuccess(10000);
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + port);
    props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);

    producer = producer(Vertx.vertx(), props);
    producer.write(new ProducerRecord<>("testBlockingBroker", 0, "key", "value"), ctx.asyncAssertFailure());
  }

  @Test
  // Should fail because it cannot reach the broker
  public void testBrokerConnectionError(TestContext ctx) throws Exception {
    Properties props = new Properties();
    // use a wrong port on purpose, because Broker IS running
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
    props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);

    producer = producer(Vertx.vertx(), props);
    producer.write(new ProducerRecord<>("testBrokerConnectionError", 0, "key", "value"), ctx.asyncAssertFailure());
  }

  @Test
  public void testExceptionHandler(TestContext ctx) throws Exception {
    Async async = ctx.async();
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    Date invalidValue = new Date();
    KafkaProducer.create(Vertx.vertx(), props).
      exceptionHandler(exception -> async.complete()).
      write(KafkaProducerRecord.create("topic", "key", invalidValue));
  }

  @Test
  public void testNotExistingPartition(TestContext ctx) {
    Async async = ctx.async(2);
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    // both exception handler and the handler on send (with failure) have to be called
    KafkaProducer.create(Vertx.vertx(), props).
      exceptionHandler(exception -> async.countDown()).
      send(KafkaProducerRecord.create("topic", null, "value", 1000), r -> async.countDown());
  }

}

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

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertTrue;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class CleanupTest extends KafkaClusterTestBase {

  private Vertx vertx;
  private int numKafkaProducerNetworkThread;
  private int numKafkaConsumerNetworkThread;
  private int numVertxKafkaConsumerThread;

  @Before
  public void beforeTest() {
    vertx = Vertx.vertx();

    // Capture thread counts, so tests that don't cleanup properly won't fail correct tests
    numKafkaProducerNetworkThread = countThreads("kafka-producer-network-thread");
    numKafkaConsumerNetworkThread = countThreads("kafka-consumer-network-thread");
    numKafkaConsumerNetworkThread = countThreads("vert.x-kafka-consumer-thread");
  }

  @After
  public void afterTest(TestContext ctx) {
    vertx.close(ctx.asyncAssertSuccess());
  }

  private int countThreads(String match) {
    return (int) Thread.getAllStackTraces().keySet()
      .stream()
      .filter(t -> t.getName().contains(match))
      .count();
  }

  private void assertNoThreads(TestContext ctx, String match) {
    ctx.assertEquals(0, countThreads(match));
  }

  private void waitUntil(BooleanSupplier supplier) {
    waitUntil(null, supplier);
  }

  private void waitUntil(String msg, BooleanSupplier supplier) {
    long now = System.currentTimeMillis();
    while (!supplier.getAsBoolean()) {
      assertTrue(msg, (System.currentTimeMillis() - now) < 10000);
    }
  }

  @Test
  public void testSharedProducer(TestContext ctx) {
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    int num = 3;
    Async sentLatch = ctx.async(num);
    LinkedList<KafkaProducer<String, String>> producers = new LinkedList<>();
    for (int i = 0;i < num;i++) {
      KafkaProducer<String, String> producer = KafkaProducer.createShared(vertx, "the-name", config);
      producer.write(KafkaProducerRecord.create("the_topic", "the_value"), ctx.asyncAssertSuccess(v -> {
        sentLatch.countDown();
      }));
      producers.add(producer);
    }
    sentLatch.awaitSuccess(10000);
    Async async = ctx.async();
    kafkaCluster.useTo().consumeStrings("the_topic", num, 10, TimeUnit.SECONDS, () -> {
      close(ctx, producers, async::complete);
    });
    async.awaitSuccess(10000);
    waitUntil(() -> countThreads("kafka-producer-network-thread") == numKafkaProducerNetworkThread);
  }

  private void close(TestContext ctx, LinkedList<KafkaProducer<String, String>> producers, Runnable doneHandler) {
    close(numKafkaProducerNetworkThread, ctx, producers, doneHandler);
  }

  private void close(int numNetworkProducerThread, TestContext ctx, LinkedList<KafkaProducer<String, String>> producers, Runnable doneHandler) {
    if (producers.size() > 0) {
      ctx.assertEquals(numNetworkProducerThread + 1, countThreads("kafka-producer-network-thread"));
      KafkaProducer<String, String> producer = producers.removeFirst();
      producer.close(ctx.asyncAssertSuccess(v -> {
        close(ctx, producers, doneHandler);
      }));
    } else {
      doneHandler.run();
    }
  }

  public static class TheVerticle extends AbstractVerticle {
    @Override
    public void start(Promise<Void> startFuture) throws Exception {
      Properties config = new Properties();
      config.putAll(context.config().getMap());
      KafkaProducer<String, String> producer = KafkaProducer.createShared(vertx, "the-name", config);
      producer.write(KafkaProducerRecord.create("the_topic", "the_value"), ar -> startFuture.handle(ar.map((Void) null)));
    }
  }

  @Test
  public void testSharedProducerCleanupInVerticle(TestContext ctx) {
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    int num = 3;
    Async sentLatch = ctx.async(num);
    AtomicReference<String> deploymentID = new AtomicReference<>();
    vertx.deployVerticle(TheVerticle.class.getName(), new DeploymentOptions().setInstances(3).setConfig(new JsonObject((Map)config)),
      ctx.asyncAssertSuccess(id -> {
        deploymentID.set(id);
        sentLatch.complete();
      }));
    sentLatch.awaitSuccess(10000);
    Async async = ctx.async();
    kafkaCluster.useTo().consumeStrings("the_topic", num, 10, TimeUnit.SECONDS, () -> {
      vertx.undeploy(deploymentID.get(), ctx.asyncAssertSuccess(v -> async.complete()));
    });
    async.awaitSuccess(10000);
    waitUntil(() -> countThreads("kafka-producer-network-thread") == numKafkaProducerNetworkThread);
  }

  @Test
  public void testCleanupInProducer(TestContext ctx) {
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    Async deployLatch = ctx.async();
    AtomicReference<KafkaProducer<String, String>> producerRef = new AtomicReference<>();
    AtomicReference<String> deploymentRef = new AtomicReference<>();
    vertx.deployVerticle(new AbstractVerticle() {
      @Override
      public void start() throws Exception {
        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, config);
        producerRef.set(producer);
        producer.write(KafkaProducerRecord.create("the_topic", "the_value"), ctx.asyncAssertSuccess());
      }
    }, ctx.asyncAssertSuccess(id -> {
      deploymentRef.set(id);
      deployLatch.complete();
    }));

    deployLatch.awaitSuccess(15000);
    Async undeployLatch = ctx.async();

    kafkaCluster.useTo().consumeStrings("the_topic", 1, 10, TimeUnit.SECONDS, () -> {
      vertx.undeploy(deploymentRef.get(), ctx.asyncAssertSuccess(v -> {
        undeployLatch.complete();
      }));
    });

    undeployLatch.awaitSuccess(10000);
    waitUntil(() -> countThreads("kafka-producer-network-thread") == numKafkaProducerNetworkThread);
  }

  @Test
  public void testCleanupInConsumer(TestContext ctx) {
    String topicName = "testCleanupInConsumer";
    Properties config = kafkaCluster.useTo().getConsumerProperties("testCleanupInConsumer_consumer",
      "testCleanupInConsumer_consumer", OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    Async async = ctx.async(2);
    Async produceLatch = ctx.async();
    vertx.deployVerticle(new AbstractVerticle() {
      boolean deployed = false;
      @Override
      public void start(Promise<Void> fut) {
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);
        deployed = true;
        consumer.handler(record -> {
          if (deployed) {
            deployed = false;
            vertx.undeploy(context.deploymentID(), ctx.asyncAssertSuccess(v2 -> async.countDown()));
          }
        });
        consumer.assign(new TopicPartition(topicName, 0), fut);
      }
    }, ctx.asyncAssertSuccess(v ->  produceLatch.complete()));
    produceLatch.awaitSuccess(10000);
    kafkaCluster.useTo().produce("testCleanupInConsumer_producer", 100,
      new StringSerializer(), new StringSerializer(), async::countDown,
      () -> new ProducerRecord<>(topicName, "the_value"));

    async.awaitSuccess(10000);
    waitUntil("Expected " + countThreads("vert.x-kafka-consumer-thread") + " == " + numVertxKafkaConsumerThread, () -> countThreads("vert.x-kafka-consumer-thread") == numKafkaConsumerNetworkThread);
  }

  @Test
  // Regression test for ISS-73: undeployment of a verticle with unassigned consumer fails
  public void testUndeployUnassignedConsumer(TestContext ctx) {
    Properties config = kafkaCluster.useTo().getConsumerProperties("testUndeployUnassignedConsumer_consumer",
      "testUndeployUnassignedConsumer_consumer", OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    Async async = ctx.async(1);
    vertx.deployVerticle(new AbstractVerticle() {
      @Override
      public void start() {
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);
      }
    }, ctx.asyncAssertSuccess(id -> {
      vertx.undeploy(id, ctx.asyncAssertSuccess(v2 -> async.complete()));
    }));

    async.awaitSuccess(10000);
    waitUntil("Expected " + countThreads("vert.x-kafka-consumer-thread") + " == " + numVertxKafkaConsumerThread, () -> countThreads("vert.x-kafka-consumer-thread") == numKafkaConsumerNetworkThread);
  }
}

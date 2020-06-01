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
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Repeat;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.producer.impl.KafkaProducerImpl;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
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
public class ProducerConsumerContextTest extends KafkaClusterTestBase {

  private Vertx vertx;
  private KafkaWriteStream<String, String> producer;
  private KafkaReadStream<String, String> consumer;

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
  public void testStreamProducerConsumerContexts(TestContext ctx) throws Exception {
    String topicName = "testStreamProduceContexts";
    Properties config = kafkaCluster.useTo().getProducerProperties("testStreamProduceContexts_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "testStreamProduceContexts_consumer");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.TRUE.toString());
    config.put(ConsumerConfig.CLIENT_ID_CONFIG, "testStreamProduceContexts_client");

    final int numMessages = 100;
    final Async async = ctx.async(numMessages * 2);

    vertx.deployVerticle(new AbstractVerticle() {
      @Override
      public void start() throws Exception {
        producer = producer(vertx, config);
        producer.exceptionHandler(ctx::fail);
        Context thisProducerCtx = context;
        vertx.deployVerticle(new AbstractVerticle() {
          @Override
          public void start() throws Exception {
            for (int i = 0; i < numMessages; i++) {
              ProducerRecord<String, String> record = new ProducerRecord<>(topicName, 0, "key-" + i, "value-" + i);
              record.headers().add("header_key", ("header_value-" + i).getBytes());
              producer.write(record, h -> {
                ctx.assertEquals(context, Vertx.currentContext());
                ctx.assertNotEquals(thisProducerCtx, Vertx.currentContext());
                async.countDown();
              });
            }
          }
        });
      }
    });

    vertx.deployVerticle(new AbstractVerticle() {
      @Override
      public void start() {
        consumer = KafkaReadStream.create(vertx, config);
        consumer.exceptionHandler(ctx::fail);
        Context thisConsumerCtx = context;

        vertx.deployVerticle(new AbstractVerticle() {
          @Override
          public void start() {
            consumer.handler(record -> {
              ctx.assertEquals(thisConsumerCtx, Vertx.currentContext());
              ctx.assertNotEquals(context, Vertx.currentContext());
              async.countDown();
            });
            consumer.subscribe(Collections.singleton(topicName));
            consumer.resume();
          }
        });
      }
    });
    async.await();
  }
}

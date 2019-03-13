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
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Producer tests
 */
public class ProducerTransactionTest extends KafkaClusterTestBase {

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
  public void testProduce(TestContext ctx) throws Exception {
    String topicName = "testProduce";
    Properties config = kafkaCluster.useTo().getProducerProperties("testProduce_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producer = producer(Vertx.vertx(), config);
    producer.exceptionHandler(ctx::fail);
    int numMessages = 100000;

    try {

      producer.initTransactions();
      producer.beginTransaction();

      for (int i = 0;i < numMessages;i++) {
        producer.write(new ProducerRecord<>(topicName, 0, "key-" + i, "value-" + i));
      }

      producer.commitTransaction();

    } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {

      // We can't recover from these exceptions, so our only option is to close the producer and exit.
      producer.close();

    } catch (KafkaException e) {

      // For all other exceptions, just abort the transaction and try again.
      producer.abortTransaction();

    }


    Async done = ctx.async();
    AtomicInteger seq = new AtomicInteger();
    kafkaCluster.useTo().consumeStrings(topicName, numMessages, 10, TimeUnit.SECONDS, done::complete, (key, value) -> {
      int count = seq.getAndIncrement();
      ctx.assertEquals("key-" + count, key);
      ctx.assertEquals("value-" + count, value);
      return true;
    });
  }

}

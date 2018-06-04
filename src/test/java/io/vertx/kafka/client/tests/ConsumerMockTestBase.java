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
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests using mock consumers
 */
@RunWith(VertxUnitRunner.class)
public abstract class ConsumerMockTestBase {

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
  public void testConsume(TestContext ctx) throws Exception {
    MockConsumer<String, String> mock = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    KafkaReadStream<String, String> consumer = createConsumer(vertx, mock);
    Async doneLatch = ctx.async();
    consumer.handler(record -> {
      ctx.assertEquals("the_topic", record.topic());
      ctx.assertEquals(0, record.partition());
      ctx.assertEquals("abc", record.key());
      ctx.assertEquals("def", record.value());
      consumer.close(v -> doneLatch.complete());
    });
    consumer.subscribe(Collections.singleton("the_topic"), v -> {
      mock.schedulePollTask(() -> {
        mock.rebalance(Collections.singletonList(new TopicPartition("the_topic", 0)));
        mock.addRecord(new ConsumerRecord<>("the_topic", 0, 0L, "abc", "def"));
        mock.seek(new TopicPartition("the_topic", 0), 0L);
      });
    });
  }

  @Test
  public void testConsumeWithHeader(TestContext ctx) {
    MockConsumer<String, String> mock = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    KafkaReadStream<String, String> consumer = createConsumer(vertx, mock);
    Async doneLatch = ctx.async();
    consumer.handler(record -> {
      ctx.assertEquals("the_topic", record.topic());
      ctx.assertEquals(0, record.partition());
      ctx.assertEquals("abc", record.key());
      ctx.assertEquals("def", record.value());
      Header[] headers = record.headers().toArray();
      ctx.assertEquals(1, headers.length);
      Header header = headers[0];
      ctx.assertEquals("header_key", header.key());
      ctx.assertEquals("header_value", new String(header.value()));
      consumer.close(v -> doneLatch.complete());
    });
    consumer.subscribe(Collections.singleton("the_topic"), v -> {
      mock.schedulePollTask(() -> {
        mock.rebalance(Collections.singletonList(new TopicPartition("the_topic", 0)));
        mock.addRecord(new ConsumerRecord<>("the_topic", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0L, 0, 0, "abc", "def",
          new RecordHeaders(Collections.singletonList(new RecordHeader("header_key", "header_value".getBytes())))));
        mock.seek(new TopicPartition("the_topic", 0), 0L);
      });
    });
  }

  @Test
  public void testBatch(TestContext ctx) throws Exception {
    int num = 50;
    MockConsumer<String, String> mock = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    KafkaReadStream<String, String> consumer = createConsumer(vertx, mock);
    Async doneLatch = ctx.async();
    AtomicInteger count = new AtomicInteger();
    consumer.handler(record -> {
      int val = count.getAndIncrement();
      if (val < num) {
        ctx.assertEquals("the_topic", record.topic());
        ctx.assertEquals(0, record.partition());
        ctx.assertEquals("key-" + val, record.key());
        ctx.assertEquals("value-" + val, record.value());
        if (val == num - 1) {
          consumer.close(v -> doneLatch.complete());
        }
      }
    });
    consumer.subscribe(Collections.singleton("the_topic"), v -> {
      mock.schedulePollTask(() -> {
        mock.rebalance(Collections.singletonList(new TopicPartition("the_topic", 0)));
        mock.seek(new TopicPartition("the_topic", 0), 0);
        for (int i = 0; i < num; i++) {
          mock.addRecord(new ConsumerRecord<>("the_topic", 0, i, "key-" + i, "value-" + i));
        }
      });
    });
  }

  abstract <K, V> KafkaReadStream<K, V> createConsumer(Vertx vertx, Consumer<K, V> consumer);
}

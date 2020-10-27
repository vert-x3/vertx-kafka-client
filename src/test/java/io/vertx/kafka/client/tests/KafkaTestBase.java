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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.client.common.KafkaClientOptions;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import org.apache.kafka.clients.producer.Producer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Base class for tests
 */
public class KafkaTestBase {

  static void close(TestContext ctx, Consumer<Handler<AsyncResult<Void>>> producer) {
    if (producer != null) {
      Async closeAsync = ctx.async();
      producer.accept(v -> {
        closeAsync.complete();
      });
      closeAsync.awaitSuccess(10000);
    }
  }

  public static void close(TestContext ctx, KafkaWriteStream<?, ?> producer) {
    if (producer != null) {
      close(ctx, handler -> producer.close(2000L, handler));
    }
  }

  public static void close(TestContext ctx, KafkaReadStream<?, ?> consumer) {
    if (consumer != null) {
      KafkaTestBase.close(ctx, consumer::close);
    }
  }

  public static Map<String, Object> mapConfig(Properties cfg) {
    Map<String, Object> map = new HashMap<>();
    cfg.forEach((k, v) -> map.put("" + k, v));
    return map;
  }

  public static <K, V> KafkaWriteStream<K, V> producer(Vertx vertx, KafkaClientOptions opts) {
    return KafkaWriteStream.create(vertx, opts);
  }

  public static <K, V> KafkaWriteStream<K, V> producer(Vertx vertx, Properties config) {
    return KafkaWriteStream.create(vertx, config);
  }

  static <K, V> KafkaWriteStream<K, V> producer(Vertx vertx, Properties config, Class<K> keyType, Class<V> valueType) {
    return KafkaWriteStream.create(vertx, config, keyType, valueType);
  }

  static <K, V> KafkaWriteStream<K, V> producer(Vertx vertx, Producer<K, V> producer) {
    return KafkaWriteStream.create(vertx, producer);
  }
}

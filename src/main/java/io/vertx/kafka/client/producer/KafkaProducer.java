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

package io.vertx.kafka.client.producer;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.WriteStream;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.producer.impl.KafkaProducerImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Vert.x Kafka producer
 */
@VertxGen
public interface KafkaProducer<K, V> extends WriteStream<KafkaProducerRecord<K, V>> {

  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Map<String, String> config) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, new HashMap<>(config));
    return new KafkaProducerImpl<>(stream);
  }

  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Map<String, String> config, Class<K> keyType, Class<V> valueType) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, new HashMap<>(config), keyType, valueType);
    return new KafkaProducerImpl<>(stream);
  }

  @GenIgnore
  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Properties config) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, config);
    return new KafkaProducerImpl<>(stream);
  }

  @GenIgnore
  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Properties config, Class<K> keyType, Class<V> valueType) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, config, keyType, valueType);
    return new KafkaProducerImpl<>(stream);
  }

  @Fluent
  @Override
  KafkaProducer<K, V> exceptionHandler(Handler<Throwable> handler);

  @Fluent
  @Override
  KafkaProducer<K, V> write(KafkaProducerRecord<K, V> kafkaProducerRecord);

  @Override
  void end();

  @Override
  void end(KafkaProducerRecord<K, V> kafkaProducerRecord);

  @Fluent
  @Override
  KafkaProducer<K, V> setWriteQueueMaxSize(int i);

  @Override
  boolean writeQueueFull();

  @Fluent
  @Override
  KafkaProducer<K, V> drainHandler(Handler<Void> handler);

  @Fluent
  KafkaProducer<K, V> write(KafkaProducerRecord<K, V> kafkaProducerRecord, Handler<RecordMetadata> handler);

  @Fluent
  KafkaProducer<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler);

  @Fluent
  KafkaProducer<K, V> flush(Handler<Void> completionHandler);

  void close();

  void close(long timeout, Handler<Void> completionHandler);

  @GenIgnore
  KafkaWriteStream<K, V> asStream();
}

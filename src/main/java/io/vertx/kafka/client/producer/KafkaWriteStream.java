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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.WriteStream;
import io.vertx.kafka.client.producer.impl.KafkaWriteStreamImpl;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A {@link WriteStream} for writing to Kafka {@link ProducerRecord}.
 * <p>
 * The {@code write()} provides global control over writing a record.
 * <p>
 */
public interface KafkaWriteStream<K, V> extends WriteStream<ProducerRecord<K, V>> {

  int DEFAULT_MAX_SIZE = 1024 * 1024;

  static <K, V> KafkaWriteStream<K, V> create(Vertx vertx, Properties config) {
    return KafkaWriteStreamImpl.create(vertx, config);
  }

  static <K, V> KafkaWriteStream<K, V> create(Vertx vertx, Properties config, Class<K> keyType, Class<V> valueType) {
    return KafkaWriteStreamImpl.create(vertx, config, keyType, valueType);
  }

  static <K, V> KafkaWriteStream<K, V> create(Vertx vertx, Map<String, Object> config) {
    return KafkaWriteStreamImpl.create(vertx, config);
  }

  static <K, V> KafkaWriteStream<K, V> create(Vertx vertx, Map<String, Object> config, Class<K> keyType, Class<V> valueType) {
    return KafkaWriteStreamImpl.create(vertx, config, keyType, valueType);
  }

  static <K, V> void create(Vertx vertx, Properties config, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    KafkaWriteStreamImpl.create(vertx, config, handler);
  }

  static <K, V> void create(Vertx vertx, Properties config, Class<K> keyType, Class<V> valueType, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    KafkaWriteStreamImpl.create(vertx, config, keyType, valueType, handler);
  }

  static <K, V> void create(Vertx vertx, Map<String, Object> config, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    KafkaWriteStreamImpl.create(vertx, config, handler);
  }

  static <K, V> void create(Vertx vertx, Map<String, Object> config, Class<K> keyType, Class<V> valueType, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    KafkaWriteStreamImpl.create(vertx, config, keyType, valueType, handler);
  }

  static <K, V> void create(Vertx vertx, Producer<K, V> producer, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    KafkaWriteStreamImpl.create(vertx, producer, handler);
  }

  KafkaWriteStream<K, V> write(ProducerRecord<K, V> record, Handler<RecordMetadata> handler);

  KafkaWriteStream<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler);

  KafkaWriteStream<K, V> flush(Handler<Void> completionHandler);

  void close();

  void close(long timeout, Handler<Void> completionHandler);
}

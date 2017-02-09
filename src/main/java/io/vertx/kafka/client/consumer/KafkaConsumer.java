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

package io.vertx.kafka.client.consumer;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.ReadStream;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerImpl;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Vert.x Kafka consumer
 */
@VertxGen
public interface KafkaConsumer<K, V> extends ReadStream<KafkaConsumerRecord<K, V>> {

  static <K, V> KafkaConsumer<K, V> create(Vertx vertx, Map<String, String> config) {
    KafkaReadStream<K, V> stream = KafkaReadStream.create(vertx, new HashMap<>(config));
    return new KafkaConsumerImpl<>(stream);
  }

  static <K, V> KafkaConsumer<K, V> create(Vertx vertx, Map<String, String> config,
                                           Class<K> keyType, Class<V> valueType) {
    KafkaReadStream<K, V> stream = KafkaReadStream.create(vertx, new HashMap<>(config), keyType, valueType);
    return new KafkaConsumerImpl<>(stream);
  }

  @GenIgnore
  static <K, V> KafkaConsumer<K, V> create(Vertx vertx, Properties config) {
    KafkaReadStream<K, V> stream = KafkaReadStream.create(vertx, config);
    return new KafkaConsumerImpl<>(stream);
  }

  @GenIgnore
  static <K, V> KafkaConsumer<K, V> create(Vertx vertx, Properties config,
                                           Class<K> keyType, Class<V> valueType) {
    KafkaReadStream<K, V> stream = KafkaReadStream.create(vertx, config, keyType, valueType);
    return new KafkaConsumerImpl<>(stream);
  }

  @Fluent
  @Override
  KafkaConsumer<K, V> exceptionHandler(Handler<Throwable> handler);

  @Fluent
  @Override
  KafkaConsumer<K, V> handler(Handler<KafkaConsumerRecord<K, V>> handler);

  @Fluent
  @Override
  KafkaConsumer<K, V> pause();

  @Fluent
  @Override
  KafkaConsumer<K, V> resume();

  @Fluent
  @Override
  KafkaConsumer<K, V> endHandler(Handler<Void> endHandler);

  @Fluent
  KafkaConsumer<K, V> subscribe(Set<String> topics);

  @Fluent
  KafkaConsumer<K, V> subscribe(Set<String> topics, Handler<AsyncResult<Void>> completionHandler);

  @Fluent
  KafkaConsumer<K, V> assign(Set<TopicPartition> topicPartitions);

  @Fluent
  KafkaConsumer<K, V> assign(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler);

  @Fluent
  KafkaConsumer<K, V> assignment(Handler<AsyncResult<Set<TopicPartition>>> handler);

  @Fluent
  @GenIgnore
  KafkaConsumer<K, V> listTopics(Handler<AsyncResult<Map<String,List<PartitionInfo>>>> handler);

  @Fluent
  KafkaConsumer<K, V> unsubscribe();

  @Fluent
  KafkaConsumer<K, V> unsubscribe(Handler<AsyncResult<Void>> completionHandler);

  @Fluent
  KafkaConsumer<K, V> subscription(Handler<AsyncResult<Set<String>>> handler);

  @Fluent
  KafkaConsumer<K, V> pause(Set<TopicPartition> topicPartitions);

  @Fluent
  KafkaConsumer<K, V> pause(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler);

  @Fluent
  KafkaConsumer<K, V> paused(Handler<AsyncResult<Set<TopicPartition>>> handler);

  @Fluent
  KafkaConsumer<K, V> resume(Set<TopicPartition> topicPartitions);

  @Fluent
  KafkaConsumer<K, V> resume(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler);

  @Fluent
  KafkaConsumer<K, V> partitionsRevokedHandler(Handler<Set<TopicPartition>> handler);

  @Fluent
  KafkaConsumer<K, V> partitionsAssignedHandler(Handler<Set<TopicPartition>> handler);

  @Fluent
  KafkaConsumer<K, V> seek(TopicPartition topicPartition, long offset);

  @Fluent
  KafkaConsumer<K, V> seek(TopicPartition topicPartition, long offset, Handler<AsyncResult<Void>> completionHandler);

  @Fluent
  @GenIgnore
  KafkaConsumer<K, V> seekToBeginning(Collection<TopicPartition> topicPartitions);

  @Fluent
  @GenIgnore
  KafkaConsumer<K, V> seekToBeginning(Collection<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler);

  @Fluent
  @GenIgnore
  KafkaConsumer<K, V> seekToEnd(Collection<TopicPartition> topicPartitions);

  @Fluent
  @GenIgnore
  KafkaConsumer<K, V> seekToEnd(Collection<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler);

  void commit();

  void commit(Handler<AsyncResult<Void>> completionHandler);

  void committed(TopicPartition topicPartition, Handler<AsyncResult<OffsetAndMetadata>> handler);

  @Fluent
  KafkaConsumer<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler);

  default void close() {
    close(null);
  }

  void close(Handler<Void> completionHandler);

  @GenIgnore
  KafkaReadStream<K, V> asStream();

}

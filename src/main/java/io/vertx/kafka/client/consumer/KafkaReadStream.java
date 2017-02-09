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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.ReadStream;
import io.vertx.kafka.client.KafkaCodecs;
import io.vertx.kafka.client.consumer.impl.KafkaReadStreamImpl;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A {@link ReadStream} for consuming Kafka {@link ConsumerRecord}.
 * <p>
 * The {@code pause()} and {@code resume()} provides global control over reading the records from the consumer.
 * <p>
 * The {@link #pause(Collection)} and {@link #resume(Collection)} provides finer grained control over reading records
 * for specific Topic/Partition, these are Kafka's specific operations.
 *
 */
public interface KafkaReadStream<K, V> extends ReadStream<ConsumerRecord<K, V>> {

  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, Properties config) {
    return create(vertx, new org.apache.kafka.clients.consumer.KafkaConsumer<>(config));
  }

  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, Properties config, Class<K> keyType, Class<V> valueType) {
    Deserializer<K> keyDeserializer = KafkaCodecs.deserializer(keyType);
    Deserializer<V> valueDeserializer = KafkaCodecs.deserializer(valueType);
    return create(vertx, new org.apache.kafka.clients.consumer.KafkaConsumer<>(config, keyDeserializer, valueDeserializer));
  }

  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, Map<String, Object> config) {
    return create(vertx, new org.apache.kafka.clients.consumer.KafkaConsumer<>(config));
  }

  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, Map<String, Object> config, Class<K> keyType, Class<V> valueType) {
    Deserializer<K> keyDeserializer = KafkaCodecs.deserializer(keyType);
    Deserializer<V> valueDeserializer = KafkaCodecs.deserializer(valueType);
    return create(vertx, new org.apache.kafka.clients.consumer.KafkaConsumer<>(config, keyDeserializer, valueDeserializer));
  }

  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, Consumer<K, V> consumer) {
    return new KafkaReadStreamImpl<>(vertx.getOrCreateContext(), consumer);
  }

  void committed(TopicPartition topicPartition, Handler<AsyncResult<OffsetAndMetadata>> handler);

  KafkaReadStream<K, V> pause(Collection<TopicPartition> topicPartitions);

  KafkaReadStream<K, V> pause(Collection<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler);

  void paused(Handler<AsyncResult<Set<TopicPartition>>> handler);

  KafkaReadStream<K, V> resume(Collection<TopicPartition> topicPartitions);

  KafkaReadStream<K, V> resume(Collection<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler);

  KafkaReadStream<K, V> seekToEnd(Collection<TopicPartition> topicPartitions);

  KafkaReadStream<K, V> seekToEnd(Collection<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler);

  KafkaReadStream<K, V> seekToBeginning(Collection<TopicPartition> topicPartitions);

  KafkaReadStream<K, V> seekToBeginning(Collection<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler);

  KafkaReadStream<K, V> seek(TopicPartition topicPartition, long offset);

  KafkaReadStream<K, V> seek(TopicPartition topicPartition, long offset, Handler<AsyncResult<Void>> completionHandler);

  KafkaReadStream<K, V> partitionsRevokedHandler(Handler<Collection<TopicPartition>> handler);

  KafkaReadStream<K, V> partitionsAssignedHandler(Handler<Collection<TopicPartition>> handler);

  KafkaReadStream<K, V> subscribe(Set<String> topics);

  KafkaReadStream<K, V> subscribe(Set<String> topics, Handler<AsyncResult<Void>> completionHandler);

  KafkaReadStream<K, V> unsubscribe();

  KafkaReadStream<K, V> unsubscribe(Handler<AsyncResult<Void>> completionHandler);

  KafkaReadStream<K, V> subscription(Handler<AsyncResult<Set<String>>> handler);

  KafkaReadStream<K, V> assign(Collection<TopicPartition> partitions);

  KafkaReadStream<K, V> assign(Collection<TopicPartition> partitions, Handler<AsyncResult<Void>> completionHandler);

  KafkaReadStream<K, V> assignment(Handler<AsyncResult<Set<TopicPartition>>> handler);

  KafkaReadStream<K, V> listTopics(Handler<AsyncResult<Map<String,List<PartitionInfo>>>> handler);

  void commit();

  void commit(Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler);

  void commit(Map<TopicPartition, OffsetAndMetadata> offsets);

  void commit(Map<TopicPartition, OffsetAndMetadata> offsets, Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler);

  KafkaReadStream<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler);

  default void close() {
    close(null);
  }

  void close(Handler<Void> completionHandler);

}

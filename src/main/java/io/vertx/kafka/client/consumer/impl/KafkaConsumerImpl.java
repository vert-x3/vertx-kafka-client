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

package io.vertx.kafka.client.consumer.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.kafka.client.common.impl.Helper;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Vert.x Kafka consumer implementation
 */
public class KafkaConsumerImpl<K, V> implements KafkaConsumer<K, V> {

  private final KafkaReadStream<K, V> stream;

  public KafkaConsumerImpl(KafkaReadStream<K, V> stream) {
    this.stream = stream;
  }

  @Override
  public KafkaConsumer<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.stream.exceptionHandler(handler);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> handler(Handler<KafkaConsumerRecord<K, V>> handler) {
    if (handler != null) {
      this.stream.handler(record -> handler.handle(new KafkaConsumerRecordImpl<>(record)));
    } else {
      this.stream.handler(null);
    }
    return this;
  }

  @Override
  public KafkaConsumer<K, V> pause() {
    this.stream.pause();
    return this;
  }

  @Override
  public KafkaConsumer<K, V> resume() {
    this.stream.resume();
    return this;
  }

  @Override
  public KafkaConsumer<K, V> pause(Set<TopicPartition> topicPartitions) {
    return this.pause(topicPartitions, null);
  }

  @Override
  public KafkaConsumer<K, V> pause(TopicPartition topicPartition, Handler<AsyncResult<Void>> completionHandler) {
    return this.pause(Collections.singleton(topicPartition), completionHandler);
  }

  @Override
  public KafkaConsumer<K, V> pause(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
    this.stream.pause(Helper.to(topicPartitions), completionHandler);
    return this;
  }

  @Override
  public void paused(Handler<AsyncResult<Set<TopicPartition>>> handler) {

    this.stream.paused(done -> {

      if (done.succeeded()) {
        handler.handle(Future.succeededFuture(Helper.from(done.result())));
      } else {
        handler.handle(Future.failedFuture(done.cause()));
      }
    });
  }

  @Override
  public KafkaConsumer<K, V> resume(TopicPartition topicPartition) {
    return this.resume(Collections.singleton(topicPartition));
  }

  @Override
  public KafkaConsumer<K, V> resume(Set<TopicPartition> topicPartitions) {
    return this.resume(topicPartitions, null);
  }

  @Override
  public KafkaConsumer<K, V> resume(TopicPartition topicPartition, Handler<AsyncResult<Void>> completionHandler) {
    return this.resume(Collections.singleton(topicPartition), completionHandler);
  }

  @Override
  public KafkaConsumer<K, V> resume(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
    this.stream.resume(Helper.to(topicPartitions), completionHandler);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> endHandler(Handler<Void> endHandler) {
    this.stream.endHandler(endHandler);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> subscribe(String topic) {
    return this.subscribe(Collections.singleton(topic));
  }

  @Override
  public KafkaConsumer<K, V> subscribe(Set<String> topics) {
    this.stream.subscribe(topics);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> subscribe(String topic, Handler<AsyncResult<Void>> completionHandler) {
    return this.subscribe(Collections.singleton(topic), completionHandler);
  }

  @Override
  public KafkaConsumer<K, V> subscribe(Set<String> topics, Handler<AsyncResult<Void>> completionHandler) {
    this.stream.subscribe(topics, completionHandler);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> assign(TopicPartition topicPartition) {
    return this.assign(Collections.singleton(topicPartition));
  }

  @Override
  public KafkaConsumer<K, V> assign(Set<TopicPartition> topicPartitions) {
    this.stream.assign(Helper.to(topicPartitions));
    return this;
  }

  @Override
  public KafkaConsumer<K, V> assign(TopicPartition topicPartition, Handler<AsyncResult<Void>> completionHandler) {
    return this.assign(Collections.singleton(topicPartition), completionHandler);
  }

  @Override
  public KafkaConsumer<K, V> assign(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
    this.stream.assign(Helper.to(topicPartitions), completionHandler);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> assignment(Handler<AsyncResult<Set<TopicPartition>>> handler) {
    this.stream.assignment(done -> {

      if (done.succeeded()) {
        handler.handle(Future.succeededFuture(Helper.from(done.result())));
      } else {
        handler.handle(Future.failedFuture(done.cause()));
      }

    });
    return this;
  }

  @Override
  public KafkaConsumer<K, V> listTopics(Handler<AsyncResult<Map<String,List<PartitionInfo>>>> handler) {
    this.stream.listTopics(done -> {

      if (done.succeeded()) {
        // TODO: use Helper class and stream approach
        Map<String,List<PartitionInfo>> topics = new HashMap<>();

        for (Map.Entry<String,List<org.apache.kafka.common.PartitionInfo>> topicEntry: done.result().entrySet()) {

          List<PartitionInfo> partitions = new ArrayList<>();

          for (org.apache.kafka.common.PartitionInfo kafkaPartitionInfo: topicEntry.getValue()) {

            PartitionInfo partitionInfo = new PartitionInfo();

            partitionInfo
              .setInSyncReplicas(
                Stream.of(kafkaPartitionInfo.inSyncReplicas()).map(Helper::from).collect(Collectors.toList()))
              .setLeader(Helper.from(kafkaPartitionInfo.leader()))
              .setPartition(kafkaPartitionInfo.partition())
              .setReplicas(
                Stream.of(kafkaPartitionInfo.replicas()).map(Helper::from).collect(Collectors.toList()))
              .setTopic(kafkaPartitionInfo.topic());

            partitions.add(partitionInfo);

          }

          topics.put(topicEntry.getKey(), partitions);
        }
        handler.handle(Future.succeededFuture(topics));
      } else {
        handler.handle(Future.failedFuture(done.cause()));
      }

    });
    return this;
  }

  @Override
  public KafkaConsumer<K, V> unsubscribe() {
    this.stream.unsubscribe();
    return this;
  }

  @Override
  public KafkaConsumer<K, V> unsubscribe(Handler<AsyncResult<Void>> completionHandler) {
    this.stream.unsubscribe(completionHandler);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> subscription(Handler<AsyncResult<Set<String>>> handler) {
    this.stream.subscription(handler);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> pause(TopicPartition topicPartition) {
    return this.pause(Collections.singleton(topicPartition));
  }

  @Override
  public KafkaConsumer<K, V> partitionsRevokedHandler(Handler<Set<TopicPartition>> handler) {
    this.stream.partitionsRevokedHandler(Helper.adaptHandler(handler));
    return this;
  }

  @Override
  public KafkaConsumer<K, V> partitionsAssignedHandler(Handler<Set<TopicPartition>> handler) {
    this.stream.partitionsAssignedHandler(Helper.adaptHandler(handler));
    return this;
  }

  @Override
  public KafkaConsumer<K, V> seek(TopicPartition topicPartition, long offset) {
    this.stream.seek(Helper.to(topicPartition), offset);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> seek(TopicPartition topicPartition, long offset, Handler<AsyncResult<Void>> completionHandler) {
    this.stream.seek(Helper.to(topicPartition), offset, completionHandler);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> seekToBeginning(TopicPartition topicPartition) {
    return this.seekToBeginning(Collections.singleton(topicPartition));
  }

  @Override
  public KafkaConsumer<K, V> seekToBeginning(Set<TopicPartition> topicPartitions) {
    this.stream.seekToBeginning(Helper.to(topicPartitions));
    return this;
  }

  @Override
  public KafkaConsumer<K, V> seekToBeginning(TopicPartition topicPartition, Handler<AsyncResult<Void>> completionHandler) {
    return this.seekToBeginning(Collections.singleton(topicPartition), completionHandler);
  }

  @Override
  public KafkaConsumer<K, V> seekToBeginning(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
    this.stream.seekToBeginning(Helper.to(topicPartitions), completionHandler);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> seekToEnd(TopicPartition topicPartition) {
    return this.seekToEnd(Collections.singleton(topicPartition));
  }

  @Override
  public KafkaConsumer<K, V> seekToEnd(Set<TopicPartition> topicPartitions) {
    this.stream.seekToEnd(Helper.to(topicPartitions));
    return this;
  }

  @Override
  public KafkaConsumer<K, V> seekToEnd(TopicPartition topicPartition, Handler<AsyncResult<Void>> completionHandler) {
    return this.seekToEnd(Collections.singleton(topicPartition), completionHandler);
  }

  @Override
  public KafkaConsumer<K, V> seekToEnd(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
    this.stream.seekToEnd(Helper.to(topicPartitions), completionHandler);
    return this;
  }

  @Override
  public void commit() {
    this.stream.commit();
  }

  @Override
  public void commit(Handler<AsyncResult<Void>> completionHandler) {
    this.stream.commit(completionHandler != null ? ar -> ar.map((Object) null) : null);
  }

  @Override
  public void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    this.stream.commit(Helper.to(offsets));
  }

  @Override
  public void commit(Map<TopicPartition, OffsetAndMetadata> offsets, Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler) {

    this.stream.commit(Helper.to(offsets), done -> {

      if (done.succeeded()) {

        completionHandler.handle(Future.succeededFuture(Helper.from(done.result())));
      } else {
        completionHandler.handle(Future.failedFuture(done.cause()));
      }

    });
  }

  @Override
  public void committed(TopicPartition topicPartition, Handler<AsyncResult<OffsetAndMetadata>> handler) {
    this.stream.committed(Helper.to(topicPartition), done -> {

      if (done.succeeded()) {
        handler.handle(Future.succeededFuture(Helper.from(done.result())));
      } else {
        handler.handle(Future.failedFuture(done.cause()));
      }
    });
  }

  @Override
  public KafkaConsumer<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler) {

    this.stream.partitionsFor(topic, done -> {

      if (done.succeeded()) {
        // TODO: use Helper class and stream approach
        List<PartitionInfo> partitions = new ArrayList<>();
        for (org.apache.kafka.common.PartitionInfo kafkaPartitionInfo: done.result()) {

          PartitionInfo partitionInfo = new PartitionInfo();

          partitionInfo
            .setInSyncReplicas(
              Stream.of(kafkaPartitionInfo.inSyncReplicas()).map(Helper::from).collect(Collectors.toList()))
            .setLeader(Helper.from(kafkaPartitionInfo.leader()))
            .setPartition(kafkaPartitionInfo.partition())
            .setReplicas(
              Stream.of(kafkaPartitionInfo.replicas()).map(Helper::from).collect(Collectors.toList()))
            .setTopic(kafkaPartitionInfo.topic());

          partitions.add(partitionInfo);
        }
        handler.handle(Future.succeededFuture(partitions));
      } else {
        handler.handle(Future.failedFuture(done.cause()));
      }
    });
    return this;
  }

  @Override
  public void close(Handler<Void> completionHandler) {
    this.stream.close(completionHandler);
  }

  @Override
  public void position(TopicPartition partition, Handler<AsyncResult<Long>> handler) {
    this.stream.position(Helper.to(partition), handler);
  }

  @Override
  public KafkaReadStream<K, V> asStream() {
    return this.stream;
  }

  @Override
  public Consumer<K, V> consumer() {
    return this.stream.consumer();
  }
}

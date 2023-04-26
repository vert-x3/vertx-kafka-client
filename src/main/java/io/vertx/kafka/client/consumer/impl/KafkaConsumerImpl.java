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

import io.vertx.core.*;
import io.vertx.core.impl.ContextInternal;
import io.vertx.kafka.client.consumer.OffsetAndTimestamp;
import io.vertx.kafka.client.common.impl.CloseHandler;
import io.vertx.kafka.client.common.impl.Helper;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.Consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Vert.x Kafka consumer implementation
 */
public class KafkaConsumerImpl<K, V> implements KafkaConsumer<K, V> {

  private static final Function<Map<?, Object>, Object> mapLongFunction = done -> done.values().stream().findFirst().get();

  private static <K, V> Function<Map<K, V>, V> foo() {
    return (Function) mapLongFunction;
  }

  private final KafkaReadStream<K, V> stream;
  private final CloseHandler closeHandler;

  public KafkaConsumerImpl(KafkaReadStream<K, V> stream) {
    this.stream = stream;
    this.closeHandler = new CloseHandler((timeout, ar) -> stream.close().onComplete(ar));
  }

  public synchronized KafkaConsumerImpl<K, V> registerCloseHook() {
    Context context = Vertx.currentContext();
    if (context == null) {
      return this;
    }
    closeHandler.registerCloseHook((ContextInternal) context);
    return this;
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
  public KafkaConsumer<K, V> fetch(long amount) {
    this.stream.fetch(amount);
    return this;
  }

  @Override
  public long demand() {
    return this.stream.demand();
  }

  @Override
  public Future<Void> pause(Set<TopicPartition> topicPartitions) {
    return this.stream.pause(Helper.to(topicPartitions));
  }

  @Override
  public Future<Set<TopicPartition>> paused() {
    return stream.paused().map(Helper::from);
  }

  @Override
  public Future<Void> resume(TopicPartition topicPartition) {
    return this.resume(Collections.singleton(topicPartition));
  }

  @Override
  public Future<Void> resume(Set<TopicPartition> topicPartitions) {
    return stream.resume(Helper.to(topicPartitions));
  }

  @Override
  public KafkaConsumer<K, V> endHandler(Handler<Void> endHandler) {
    this.stream.endHandler(endHandler);
    return this;
  }

  @Override
  public Future<Void> subscribe(String topic) {
    return this.subscribe(Collections.singleton(topic));
  }

  @Override
  public Future<Void> subscribe(Set<String> topics) {
    return this.stream.subscribe(topics);
  }

  @Override
  public Future<Void> subscribe(Pattern pattern) {
    return this.stream.subscribe(pattern);
  }

  @Override
  public Future<Void> assign(TopicPartition topicPartition) {
    return this.assign(Collections.singleton(topicPartition));
  }

  @Override
  public Future<Void> assign(Set<TopicPartition> topicPartitions) {
    return this.stream.assign(Helper.to(topicPartitions));
  }

  @Override
  public Future<Set<TopicPartition>> assignment() {
    return this.stream.assignment().map(Helper::from);
  }

  @Override
  public Future<Map<String, List<PartitionInfo>>> listTopics() {
    return this.stream.listTopics().map(done -> {
      // TODO: use Helper class and stream approach
      Map<String,List<PartitionInfo>> topics = new HashMap<>();

      for (Map.Entry<String,List<org.apache.kafka.common.PartitionInfo>> topicEntry: done.entrySet()) {

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
      return topics;
    });
  }

  @Override
  public Future<Void> unsubscribe() {
    return this.stream.unsubscribe();
  }

  @Override
  public Future<Set<String>> subscription() {
    return this.stream.subscription();
  }

  @Override
  public Future<Void> pause(TopicPartition topicPartition) {
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
  public Future<Void> seek(TopicPartition topicPartition, long offset) {
    return this.stream.seek(Helper.to(topicPartition), offset);
  }

  @Override
  public Future<Void> seek(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
    return seek(topicPartition, offsetAndMetadata.getOffset());
  }

  @Override
  public Future<Void> seekToBeginning(TopicPartition topicPartition) {
    return this.seekToBeginning(Collections.singleton(topicPartition));
  }

  @Override
  public Future<Void> seekToBeginning(Set<TopicPartition> topicPartitions) {
    return this.stream.seekToBeginning(Helper.to(topicPartitions));
  }

  @Override
  public Future<Void> seekToEnd(TopicPartition topicPartition) {
    return this.seekToEnd(Collections.singleton(topicPartition));
  }

  @Override
  public Future<Void> seekToEnd(Set<TopicPartition> topicPartitions) {
    return this.stream.seekToEnd(Helper.to(topicPartitions));
  }

  @Override
  public Future<Void> commit() {
    return this.stream.commit().mapEmpty();
  }

  @Override
  public Future<Map<TopicPartition, OffsetAndMetadata>> commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    return this.stream.commit(Helper.to(offsets)).map(Helper::from);
  }

  @Override
  public Future<OffsetAndMetadata> committed(TopicPartition topicPartition) {
    return this.stream.committed(Helper.to(topicPartition)).map(Helper::from);
  }

  @Override
  public Future<List<PartitionInfo>> partitionsFor(String topic) {
    return this.stream.partitionsFor(topic).map(done -> {
      // TODO: use Helper class and stream approach
      List<PartitionInfo> partitions = new ArrayList<>();
      for (org.apache.kafka.common.PartitionInfo kafkaPartitionInfo: done) {

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
      return partitions;
    });
  }

  @Override
  public Future<Void> close() {
    Promise<Void> promise = Promise.promise();
    this.closeHandler.close(promise);
    return promise.future();
  }

  @Override
  public Future<Long> position(TopicPartition partition) {
    return this.stream.position(Helper.to(partition));
  }

  @Override
  public Future<OffsetAndTimestamp> offsetsForTimes(TopicPartition topicPartition, Long timestamp) {
    Map<TopicPartition, Long> topicPartitions = new HashMap<>();
    topicPartitions.put(topicPartition, timestamp);

    return this.stream.offsetsForTimes(Helper.toTopicPartitionTimes(topicPartitions)).map(done -> {
      if (done.values().size() == 1) {
        org.apache.kafka.common.TopicPartition kTopicPartition = new org.apache.kafka.common.TopicPartition (topicPartition.getTopic(), topicPartition.getPartition());
        org.apache.kafka.clients.consumer.OffsetAndTimestamp offsetAndTimestamp = done.get(kTopicPartition);
        if(offsetAndTimestamp != null) {
          OffsetAndTimestamp resultOffsetAndTimestamp = new OffsetAndTimestamp(offsetAndTimestamp.offset(), offsetAndTimestamp.timestamp());
          return resultOffsetAndTimestamp;
        }
        // offsetAndTimestamp is null, i.e., search by timestamp did not lead to a result
        else {
          return null;
        }
      } else if (done.values().size() == 0) {
        return null;
      } else {
        throw new VertxException("offsetsForTimes should return exactly one OffsetAndTimestamp", true);
      }
    });
  }

  @Override
  public Future<Map<TopicPartition, OffsetAndTimestamp>> offsetsForTimes(Map<TopicPartition, Long> topicPartitionTimestamps) {
    return this.stream.offsetsForTimes(Helper.toTopicPartitionTimes(topicPartitionTimestamps)).map(Helper::fromTopicPartitionOffsetAndTimestamp);
  }

  @Override
  public Future<Map<TopicPartition, Long>> beginningOffsets(Set<TopicPartition> topicPartitions) {
    return this.stream.beginningOffsets(Helper.to(topicPartitions)).map(Helper::fromTopicPartitionOffsets);
  }

  @Override
  public Future<Long> beginningOffsets(TopicPartition topicPartition) {
    Set<TopicPartition> beginningOffsets = new HashSet<>();
    beginningOffsets.add(topicPartition);
    return this.stream.beginningOffsets(Helper.to(beginningOffsets)).map(foo());
  }

  @Override
  public Future<Map<TopicPartition, Long>> endOffsets(Set<TopicPartition> topicPartitions) {
    return this.stream.endOffsets(Helper.to(topicPartitions)).map(Helper::fromTopicPartitionOffsets);
  }

  @Override
  public Future<Long> endOffsets(TopicPartition topicPartition) {
    Set<TopicPartition> topicPartitions = new HashSet<>();
    topicPartitions.add(topicPartition);
    return this.stream.endOffsets(Helper.to(topicPartitions)).map(foo());
  }

  @Override
  public KafkaReadStream<K, V> asStream() {
    return this.stream;
  }

  @Override
  public Consumer<K, V> unwrap() {
    return this.stream.unwrap();
  }

  @Override
  public KafkaConsumer<K, V> batchHandler(Handler<KafkaConsumerRecords<K, V>> handler) {
    stream.batchHandler(records -> {
      handler.handle(new KafkaConsumerRecordsImpl<>(records));
    });
    return this;
  }

  @Override
  public KafkaConsumer<K, V> pollTimeout(final Duration timeout) {
    this.stream.pollTimeout(timeout);
    return this;
  }

  @Override
  public Future<KafkaConsumerRecords<K, V>> poll(final Duration timeout) {
    return this.stream.poll(timeout).map(done -> new KafkaConsumerRecordsImpl<>(done));
  }
}

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

package io.vertx.kafka.client.producer.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.kafka.client.common.Helper;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Vert.x Kafka producer implementation
 */
public class KafkaProducerImpl<K, V> implements KafkaProducer<K, V> {

  private final KafkaWriteStream<K, V> stream;

  public KafkaProducerImpl(KafkaWriteStream<K, V> stream) {
    this.stream = stream;
  }

  @Override
  public KafkaProducer<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.stream.exceptionHandler(handler);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public KafkaProducer<K, V> write(KafkaProducerRecord<K, V> kafkaProducerRecord) {
    this.stream.write(kafkaProducerRecord.record());
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public KafkaProducer<K, V> write(KafkaProducerRecord<K, V> record, Handler<RecordMetadata> handler) {
    this.stream.write(record.record(), metadata -> {
      handler.handle(Helper.from(metadata));
    });
    return this;
  }

  @Override
  public KafkaProducer<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler) {
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
  public void end() {
    this.stream.end();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void end(KafkaProducerRecord<K, V> kafkaProducerRecord) {
    this.stream.end(kafkaProducerRecord.record());
  }

  @Override
  public KafkaProducer<K, V> setWriteQueueMaxSize(int size) {
    this.stream.setWriteQueueMaxSize(size);
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return this.stream.writeQueueFull();
  }

  @Override
  public KafkaProducer<K, V> drainHandler(Handler<Void> handler) {
    this.stream.drainHandler(handler);
    return this;
  }

  @Override
  public KafkaProducer<K, V> flush(Handler<Void> completionHandler) {
    this.stream.flush(completionHandler);
    return this;
  }

  @Override
  public void close() {
    this.stream.close();
  }

  @Override
  public void close(long timeout, Handler<Void> completionHandler) {
    this.stream.close(timeout, completionHandler);
  }

  @Override
  public KafkaWriteStream<K, V> asStream() {
    return this.stream;
  }
}

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
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import io.vertx.kafka.client.common.impl.CloseHandler;
import io.vertx.kafka.client.common.impl.Helper;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Vert.x Kafka producer implementation
 */
public class KafkaProducerImpl<K, V> implements KafkaProducer<K, V> {

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Properties config) {
    return createShared(vertx, name, () -> KafkaWriteStream.create(vertx, config));
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Map<String, String> config) {
    return createShared(vertx, name, () -> KafkaWriteStream.create(vertx, new HashMap<>(config)));
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Properties config, Class<K> keyType, Class<V> valueType) {
    return createShared(vertx, name, () -> KafkaWriteStream.create(vertx, config, keyType, valueType));
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Map<String, String> config, Class<K> keyType, Class<V> valueType) {
    return createShared(vertx, name, () -> KafkaWriteStream.create(vertx, new HashMap<>(config), keyType, valueType));
  }

  private static class SharedProducer extends HashMap<Object, KafkaProducer> {

    final Producer producer;
    final CloseHandler closeHandler;

    public SharedProducer(KafkaWriteStream stream) {
      this.producer = stream.unwrap();
      this.closeHandler = new CloseHandler(stream::close);
    }
  }

  private static final Map<String, SharedProducer> sharedProducers = new HashMap<>();

  private static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Supplier<KafkaWriteStream> streamFactory) {
    synchronized (sharedProducers) {
      SharedProducer sharedProducer = sharedProducers.computeIfAbsent(name, key -> {
        KafkaWriteStream stream = streamFactory.get();
        SharedProducer s = new SharedProducer(stream);
        s.closeHandler.registerCloseHook((VertxInternal) vertx);
        return s;
      });
      Object key = new Object();
      KafkaProducerImpl<K, V> producer = new KafkaProducerImpl<>(KafkaWriteStream.create(vertx, sharedProducer.producer), new CloseHandler((timeout, ar) -> {
        synchronized (sharedProducers) {
          sharedProducer.remove(key);
          if (sharedProducer.isEmpty()) {
            sharedProducers.remove(name);
            sharedProducer.closeHandler.close(timeout, ar);
            return;
          }
        }
        ar.handle(Future.succeededFuture());
      }));
      sharedProducer.put(key, producer);
      return producer.registerCloseHook();
    }
  }

  private final KafkaWriteStream<K, V> stream;
  private final CloseHandler closeHandler;

  public KafkaProducerImpl(KafkaWriteStream<K, V> stream, CloseHandler closeHandler) {
    this.stream = stream;
    this.closeHandler = closeHandler;
  }

  public KafkaProducerImpl(KafkaWriteStream<K, V> stream) {
    this(stream, new CloseHandler(stream::close));
  }

  public KafkaProducerImpl<K, V> registerCloseHook() {
    Context context = Vertx.currentContext();
    if (context == null) {
      return this;
    }
    closeHandler.registerCloseHook(context);
    return this;
  }

  @Override
  public KafkaProducer<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.stream.exceptionHandler(handler);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public KafkaProducer<K, V> write(KafkaProducerRecord<K, V> kafkaProducerRecord) {
    return this.write(kafkaProducerRecord, null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public KafkaProducer<K, V> write(KafkaProducerRecord<K, V> record, Handler<AsyncResult<RecordMetadata>> handler) {
    this.stream.write(record.record(), done -> {
      if (handler != null) {
        if (done.succeeded()) {
          handler.handle(Future.succeededFuture(Helper.from(done.result())));
        } else {
          handler.handle(Future.failedFuture(done.cause()));
        }
      }
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
    closeHandler.close();
  }

  @Override
  public void close(Handler<AsyncResult<Void>> completionHandler) {
    closeHandler.close(completionHandler);
  }

  @Override
  public void close(long timeout, Handler<AsyncResult<Void>> completionHandler) {
    closeHandler.close(completionHandler);
  }

  @Override
  public KafkaWriteStream<K, V> asStream() {
    return this.stream;
  }

  @Override
  public Producer<K, V> unwrap() {
    return this.stream.unwrap();
  }
}

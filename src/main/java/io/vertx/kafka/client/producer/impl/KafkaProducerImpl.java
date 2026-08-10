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

import io.vertx.core.*;
import io.vertx.core.internal.Closeable;
import io.vertx.core.internal.CloseableResource;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.VertxInternal;
import io.vertx.kafka.client.common.KafkaClientOptions;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.impl.Helper;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.producer.RecordMetadata;
import io.vertx.kafka.client.serialization.VertxSerdes;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
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
    return createShared(
      vertx,
      name,
      () -> new org.apache.kafka.clients.producer.KafkaProducer<>(config),
      KafkaClientOptions.fromProperties(config, true));
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Map<String, String> config) {
    Map<String, Object> copy = new HashMap<>(config);
    return createShared(
      vertx,
      name,
      () -> new org.apache.kafka.clients.producer.KafkaProducer<>(copy),
      KafkaClientOptions.fromMap(copy, true));
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, KafkaClientOptions options) {
    Map<String, Object> config = new HashMap<>();
    if (options.getConfig() != null) {
      config.putAll(options.getConfig());
    }
    return createShared(vertx, name, () -> new org.apache.kafka.clients.producer.KafkaProducer<>(config), options);
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Properties config, Class<K> keyType, Class<V> valueType) {
    Serializer<K> keySerializer = VertxSerdes.serdeFrom(keyType).serializer();
    Serializer<V> valueSerializer = VertxSerdes.serdeFrom(valueType).serializer();
    return createShared(vertx, name, config, keySerializer, valueSerializer);
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Properties config, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    KafkaClientOptions options = KafkaClientOptions.fromProperties(config, true);
    return createShared(
      vertx,
      name,
      () -> new org.apache.kafka.clients.producer.KafkaProducer<>(config, keySerializer, valueSerializer),
      options);
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Map<String, String> config, Class<K> keyType, Class<V> valueType) {
    Serializer<K> keySerializer = VertxSerdes.serdeFrom(keyType).serializer();
    Serializer<V> valueSerializer = VertxSerdes.serdeFrom(valueType).serializer();
    return createShared(vertx, name, config, keySerializer, valueSerializer);
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Map<String, String> config, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    Map<String, Object> copy = new HashMap<>(config);
    return createShared(
      vertx,
      name,
      () -> new org.apache.kafka.clients.producer.KafkaProducer<>(copy, keySerializer, valueSerializer),
      KafkaClientOptions.fromMap(copy, true));
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, KafkaClientOptions options, Class<K> keyType, Class<V> valueType) {
    Serializer<K> keySerializer = VertxSerdes.serdeFrom(keyType).serializer();
    Serializer<V> valueSerializer = VertxSerdes.serdeFrom(valueType).serializer();
    return createShared(vertx, name, options, keySerializer, valueSerializer);
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, KafkaClientOptions options, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    Map<String, Object> config = new HashMap<>();
    if (options.getConfig() != null) {
      config.putAll(options.getConfig());
    }
    return createShared(vertx, name, () -> new org.apache.kafka.clients.producer.KafkaProducer<>(config, keySerializer, valueSerializer), options);
  }

  private static class SharedProducer<K, V> implements Closeable {

    private final Vertx vertx;
    private final Producer<K, V> producer;

    public SharedProducer(Vertx vertx,  Producer<K, V> producer) {
      this.vertx = vertx;
      this.producer = producer;
    }

    @Override
    public Future<Void> shutdown(Duration timeout) {
      return vertx.executeBlocking(() -> {
        producer.close();
        return null;
      });
    }
  }

  private static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Supplier<Producer<K, V>> producerFactory, KafkaClientOptions options) {
    CloseableResource<SharedProducer<K, V>> sharedProducer = ((VertxInternal) vertx).createSharedResource("__vertx.shared.kafka.producer", name, () -> {
      Producer<K, V> producer = producerFactory.get();
      return new SharedProducer<>(vertx, producer);
    });
    KafkaWriteStream<K, V> kafkaWriteStream = KafkaWriteStream.create(vertx, sharedProducer.get().producer, options);
    return new KafkaProducerImpl<>(kafkaWriteStream, sharedProducer, true);
  }

  private final KafkaWriteStream<K, V> stream;
  private final Closeable close;

  public KafkaProducerImpl(KafkaWriteStream<K, V> stream, Closeable close, boolean registerHook) {

    Context context;
    if (registerHook && (context = Vertx.currentContext()) != null) {
      close = ((ContextInternal)context).registerResource(close);
    }

    this.stream = stream;
    this.close = close;
  }

  public KafkaProducerImpl(Vertx vertx, KafkaWriteStream<K, V> stream) {
    this(vertx, stream, false);
  }

  public KafkaProducerImpl(Vertx vertx, KafkaWriteStream<K, V> stream, boolean registerHook) {
    this(stream, timeout -> vertx.executeBlocking(() -> {
      stream.close();
      return null;
    }), registerHook);
  }

  @Override
  public Future<Void> initTransactions() {
    return this.stream.initTransactions();
  }

  @Override
  public Future<Void> beginTransaction() {
    return this.stream.beginTransaction();
  }

  @Override
  public Future<Void> commitTransaction() {
    return this.stream.commitTransaction();
  }

  @Override
  public Future<Void> abortTransaction() {
    return this.stream.abortTransaction();
  }

  @Override
  public KafkaProducer<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.stream.exceptionHandler(handler);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Future<Void> write(KafkaProducerRecord<K, V> kafkaProducerRecord) {
    return this.stream.write(kafkaProducerRecord.record());
  }

  @Override
  public Future<RecordMetadata> send(KafkaProducerRecord<K, V> record) {
    return this.stream.send(record.record()).map(Helper::from);
  }

  @Override
  public Future<List<PartitionInfo>> partitionsFor(String topic) {
    return this.stream.partitionsFor(topic).map(list ->
      list.stream().map(kafkaPartitionInfo ->
          new PartitionInfo()
            .setInSyncReplicas(
              Stream.of(kafkaPartitionInfo.inSyncReplicas()).map(Helper::from).collect(Collectors.toList()))
            .setLeader(Helper.from(kafkaPartitionInfo.leader()))
            .setPartition(kafkaPartitionInfo.partition())
            .setReplicas(
              Stream.of(kafkaPartitionInfo.replicas()).map(Helper::from).collect(Collectors.toList()))
            .setTopic(kafkaPartitionInfo.topic())
        ).collect(Collectors.toList())
    );
  }

  @Override
  public Future<Void> end() {
    return stream.end();
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
  public Future<Void> flush() {
    return this.stream.flush();
  }

  @Override
  public Future<Void> close(long timeout) {
    return close.shutdown(Duration.ofMillis(timeout));
  }

  @Override
  public Future<Void> close() {
    return close.close();
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

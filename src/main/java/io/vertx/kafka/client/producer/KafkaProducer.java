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
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.streams.WriteStream;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.producer.impl.KafkaProducerImpl;
import org.apache.kafka.clients.producer.Producer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Vert.x Kafka producer.
 * <p>
 * The {@link #write(Object)} provides global control over writing a record.
 */
@VertxGen
public interface KafkaProducer<K, V> extends WriteStream<KafkaProducerRecord<K, V>> {

  /**
   * Get or create a KafkaProducer instance which shares its stream with any other KafkaProducer created with the same {@code name}
   *
   * @param vertx Vert.x instance to use
   * @param name the producer name to identify it
   * @param config  Kafka producer configuration
   * @return  an instance of the KafkaProducer
   */
  @GenIgnore
  static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Properties config) {
    return KafkaProducerImpl.createShared(vertx, name, config);
  }

  /**
   * Get or create a KafkaProducer instance which shares its stream with any other KafkaProducer created with the same {@code name}
   *
   * @param vertx Vert.x instance to use
   * @param name the producer name to identify it
   * @param config  Kafka producer configuration
   * @return  an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Map<String, String> config) {
    return KafkaProducerImpl.createShared(vertx, name, config);
  }

  /**
   * Get or create a KafkaProducer instance which shares its stream with any other KafkaProducer created with the same {@code name}
   *
   * @param vertx Vert.x instance to use
   * @param name the producer name to identify it
   * @param config  Kafka producer configuration
   * @param keyType class type for the key serialization
   * @param valueType class type for the value serialization
   * @return  an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Map<String, String> config, Class<K> keyType, Class<V> valueType) {
    return KafkaProducerImpl.createShared(vertx, name, config, keyType, valueType);
  }

  /**
   * Get or create a KafkaProducer instance which shares its stream with any other KafkaProducer created with the same {@code name}
   *
   * @param vertx Vert.x instance to use
   * @param name the producer name to identify it
   * @param config  Kafka producer configuration
   * @param keyType class type for the key serialization
   * @param valueType class type for the value serialization
   * @return  an instance of the KafkaProducer
   */
  @GenIgnore
  static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Properties config, Class<K> keyType, Class<V> valueType) {
    return KafkaProducerImpl.createShared(vertx, name, config, keyType, valueType);
  }

  /**
   * Create a new KafkaProducer instance from a native {@link Producer}.
   *
   * @param vertx Vert.x instance to use
   * @param producer the Kafka producer to wrap
   * @return  an instance of the KafkaProducer
   */
  @GenIgnore
  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Producer<K, V> producer) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, producer);
    return new KafkaProducerImpl<>(stream);
  }

  /**
   * Create a new KafkaProducer instance
   *
   * @param vertx Vert.x instance to use
   * @param config  Kafka producer configuration
   * @return  an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Map<String, String> config) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, new HashMap<>(config));
    return new KafkaProducerImpl<>(stream).registerCloseHook();
  }

  /**
   * Create a new KafkaProducer instance
   *
   * @param vertx Vert.x instance to use
   * @param config  Kafka producer configuration
   * @param keyType class type for the key serialization
   * @param valueType class type for the value serialization
   * @return  an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Map<String, String> config, Class<K> keyType, Class<V> valueType) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, new HashMap<>(config), keyType, valueType);
    return new KafkaProducerImpl<>(stream).registerCloseHook();
  }

  /**
   * Create a new KafkaProducer instance
   *
   * @param vertx Vert.x instance to use
   * @param config  Kafka producer configuration
   * @return  an instance of the KafkaProducer
   */
  @GenIgnore
  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Properties config) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, config);
    return new KafkaProducerImpl<>(stream).registerCloseHook();
  }

  /**
   * Create a new KafkaProducer instance
   *
   * @param vertx Vert.x instance to use
   * @param config  Kafka producer configuration
   * @param keyType class type for the key serialization
   * @param valueType class type for the value serialization
   * @return  an instance of the KafkaProducer
   */
  @GenIgnore
  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Properties config, Class<K> keyType, Class<V> valueType) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, config, keyType, valueType);
    return new KafkaProducerImpl<>(stream).registerCloseHook();
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

  /**
   * Asynchronously write a record to a topic
   *
   * @param record  record to write
   * @param handler handler called on operation completed
   * @return  current KafkaWriteStream instance
   */
  @Fluent
  KafkaProducer<K, V> write(KafkaProducerRecord<K, V> record, Handler<AsyncResult<RecordMetadata>> handler);

  /**
   * Get the partition metadata for the give topic.
   *
   * @param topic topic partition for which getting partitions info
   * @param handler handler called on operation completed
   * @return  current KafkaProducer instance
   */
  @Fluent
  KafkaProducer<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler);

  /**
   * Invoking this method makes all buffered records immediately available to write
   *
   * @param completionHandler handler called on operation completed
   * @return  current KafkaProducer instance
   */
  @Fluent
  KafkaProducer<K, V> flush(Handler<Void> completionHandler);

  /**
   * Close the producer
   */
  void close();

  /**
   * Close the producer
   *
   * @param completionHandler handler called on operation completed
   */
  void close(Handler<AsyncResult<Void>> completionHandler);

  /**
   * Close the producer
   *
   * @param timeout timeout to wait for closing
   * @param completionHandler handler called on operation completed
   */
  void close(long timeout, Handler<AsyncResult<Void>> completionHandler);

  /**
   * @return  underlying {@link KafkaWriteStream} instance
   */
  @GenIgnore
  KafkaWriteStream<K, V> asStream();

  /**
   * @return the underlying producer
   */
  @GenIgnore
  Producer<K, V> unwrap();
}

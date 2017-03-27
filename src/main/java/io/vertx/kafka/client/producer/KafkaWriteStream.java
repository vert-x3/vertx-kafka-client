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
 * The {@link #write(Object)} provides global control over writing a record.
 * <p>
 */
public interface KafkaWriteStream<K, V> extends WriteStream<ProducerRecord<K, V>> {

  int DEFAULT_MAX_SIZE = 1024 * 1024;

  /**
   * Create a new KafkaWriteStream instance
   *
   * @param vertx Vert.x instance to use
   * @param config  Kafka producer configuration
   * @return  an instance of the KafkaWriteStream
   */
  static <K, V> KafkaWriteStream<K, V> create(Vertx vertx, Properties config) {
    return KafkaWriteStreamImpl.create(vertx, config);
  }

  /**
   * Create a new KafkaWriteStream instance
   *
   * @param vertx Vert.x instance to use
   * @param config  Kafka producer configuration
   * @param keyType class type for the key serialization
   * @param valueType class type for the value serialization
   * @return  an instance of the KafkaWriteStream
   */
  static <K, V> KafkaWriteStream<K, V> create(Vertx vertx, Properties config, Class<K> keyType, Class<V> valueType) {
    return KafkaWriteStreamImpl.create(vertx, config, keyType, valueType);
  }

  /**
   * Create a new KafkaWriteStream instance
   *
   * @param vertx Vert.x instance to use
   * @param config  Kafka producer configuration
   * @return  an instance of the KafkaWriteStream
   */
  static <K, V> KafkaWriteStream<K, V> create(Vertx vertx, Map<String, Object> config) {
    return KafkaWriteStreamImpl.create(vertx, config);
  }

  /**
   * Create a new KafkaWriteStream instance
   *
   * @param vertx Vert.x instance to use
   * @param config  Kafka producer configuration
   * @param keyType class type for the key serialization
   * @param valueType class type for the value serialization
   * @return  an instance of the KafkaWriteStream
   */
  static <K, V> KafkaWriteStream<K, V> create(Vertx vertx, Map<String, Object> config, Class<K> keyType, Class<V> valueType) {
    return KafkaWriteStreamImpl.create(vertx, config, keyType, valueType);
  }

  /**
   * Create a new KafkaWriteStream instance
   *
   * @param vertx Vert.x instance to use
   * @param producer  native Kafka producer instance
   */
  static <K, V> KafkaWriteStream<K, V> create(Vertx vertx, Producer<K, V> producer) {
    return new KafkaWriteStreamImpl<>(vertx.getOrCreateContext(), producer);
  }

  /**
   * Asynchronously write a record to a topic
   *
   * @param record  record to write
   * @param handler handler called on operation completed
   * @return  current KafkaWriteStream instance
   */
  KafkaWriteStream<K, V> write(ProducerRecord<K, V> record, Handler<AsyncResult<RecordMetadata>> handler);

  /**
   * Get the partition metadata for the give topic.
   *
   * @param topic topic partition for which getting partitions info
   * @param handler handler called on operation completed
   * @return  current KafkaWriteStream instance
   */
  KafkaWriteStream<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler);

  /**
   * Invoking this method makes all buffered records immediately available to write
   *
   * @param completionHandler handler called on operation completed
   * @return  current KafkaWriteStream instance
   */
  KafkaWriteStream<K, V> flush(Handler<Void> completionHandler);

  /**
   * Close the stream
   */
  void close();

  /**
   * Close the stream
   *
   * @param completionHandler handler called on operation completed
   */
  void close(Handler<AsyncResult<Void>> completionHandler);

  /**
   * Close the stream
   *
   * @param timeout timeout to wait for closing
   * @param completionHandler handler called on operation completed
   */
  void close(long timeout, Handler<AsyncResult<Void>> completionHandler);

  /**
   * @return the underlying producer
   */
  Producer<K, V> unwrap();
}

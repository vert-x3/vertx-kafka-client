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
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.serialization.VertxSerdes;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka write stream implementation
 */
public class KafkaWriteStreamImpl<K, V> implements KafkaWriteStream<K, V> {

  public static <K, V> KafkaWriteStreamImpl<K, V> create(Vertx vertx, Properties config) {
    return new KafkaWriteStreamImpl<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.producer.KafkaProducer<>(config));
  }

  public static <K, V> KafkaWriteStreamImpl<K, V> create(Vertx vertx, Properties config, Class<K> keyType, Class<V> valueType) {
    Serializer<K> keySerializer = VertxSerdes.serdeFrom(keyType).serializer();
    Serializer<V> valueSerializer = VertxSerdes.serdeFrom(valueType).serializer();
    return new KafkaWriteStreamImpl<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.producer.KafkaProducer<>(config, keySerializer, valueSerializer));
  }

  public static <K, V> KafkaWriteStreamImpl<K, V> create(Vertx vertx, Map<String, Object> config) {
    return new KafkaWriteStreamImpl<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.producer.KafkaProducer<>(config));
  }

  public static <K, V> KafkaWriteStreamImpl<K, V> create(Vertx vertx, Map<String, Object> config, Class<K> keyType, Class<V> valueType) {
    Serializer<K> keySerializer = VertxSerdes.serdeFrom(keyType).serializer();
    Serializer<V> valueSerializer = VertxSerdes.serdeFrom(valueType).serializer();
    return new KafkaWriteStreamImpl<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.producer.KafkaProducer<>(config, keySerializer, valueSerializer));
  }

  private long maxSize = DEFAULT_MAX_SIZE;
  private long pending;
  private final Producer<K, V> producer;
  private Handler<Void> drainHandler;
  private Handler<Throwable> exceptionHandler;
  private final Context context;

  public KafkaWriteStreamImpl(Context context, Producer<K, V> producer) {
    this.producer = producer;
    this.context = context;
  }

  private int len(Object value) {
    if (value instanceof byte[]) {
      return ((byte[])value).length;
    } else if (value instanceof String) {
      return ((String)value).length();
    } else {
      return 1;
    }
  }

  @Override
  public synchronized KafkaWriteStreamImpl<K, V> write(ProducerRecord<K, V> record, Handler<AsyncResult<RecordMetadata>> handler) {

    int len = this.len(record.value());
    this.pending += len;
    this.context.<RecordMetadata>executeBlocking(fut -> {
      try {
        this.producer.send(record, (metadata, err) -> {

          // callback from IO thread
          this.context.runOnContext(v1 -> {
            synchronized (KafkaWriteStreamImpl.this) {

              // if exception happens, no record written
              if (err != null) {

                if (this.exceptionHandler != null) {
                  Handler<Throwable> exceptionHandler = this.exceptionHandler;
                  this.context.runOnContext(v2 -> exceptionHandler.handle(err));
                }
              }

              long lowWaterMark = this.maxSize / 2;
              this.pending -= len;
              if (this.pending < lowWaterMark && this.drainHandler != null) {
                Handler<Void> drainHandler = this.drainHandler;
                this.drainHandler = null;
                this.context.runOnContext(drainHandler);
              }
            }

            if (handler != null) {
              handler.handle(err != null ? Future.failedFuture(err) : Future.succeededFuture(metadata));
            }
          });
        });
      } catch (Throwable e) {
        exceptionHandler.handle(e);
      }
    }, null);

    return this;
  }

  @Override
  public KafkaWriteStreamImpl<K, V> write(ProducerRecord<K, V> record) {

    return this.write(record, null);
  }

  @Override
  public KafkaWriteStreamImpl<K, V> setWriteQueueMaxSize(int size) {
    this.maxSize = size;
    return this;
  }

  @Override
  public synchronized boolean writeQueueFull() {
    return (this.pending >= this.maxSize);
  }

  @Override
  public synchronized KafkaWriteStreamImpl<K, V> drainHandler(Handler<Void> handler) {
    this.drainHandler = handler;
    return this;
  }

  @Override
  public void end() {
  }

  @Override
  public KafkaWriteStreamImpl<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.exceptionHandler = handler;
    return this;
  }

  @Override
  public KafkaWriteStreamImpl<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler) {

    AtomicBoolean done = new AtomicBoolean();

    // TODO: should be this timeout related to the Kafka producer property "metadata.fetch.timeout.ms" ?
    this.context.owner().setTimer(2000, id -> {
      if (done.compareAndSet(false, true)) {
        handler.handle(Future.failedFuture("Kafka connect timeout"));
      }
    });

    this.context.executeBlocking(future -> {

      List<PartitionInfo> partitions = this.producer.partitionsFor(topic);
      if (done.compareAndSet(false, true)) {
        future.complete(partitions);
      }
    }, handler);

    return this;
  }

  @Override
  public KafkaWriteStreamImpl<K, V> flush(Handler<Void> completionHandler) {

    this.context.executeBlocking(future -> {

      this.producer.flush();
      future.complete();

    }, ar -> completionHandler.handle(null));

    return this;
  }

  public void close() {
    close(ar -> {});
  }

  @Override
  public void close(Handler<AsyncResult<Void>> completionHandler) {
    close(0, completionHandler);
  }

  public void close(long timeout, Handler<AsyncResult<Void>> completionHandler) {

    this.context.executeBlocking(future -> {
      if (timeout > 0) {
        this.producer.close(timeout, TimeUnit.MILLISECONDS);
      } else {
        this.producer.close();
      }
      future.complete();
    }, completionHandler);
  }

  @Override
  public Producer<K, V> unwrap() {
    return this.producer;
  }
}

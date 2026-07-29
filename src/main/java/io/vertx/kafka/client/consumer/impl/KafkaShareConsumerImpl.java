/*
 * Copyright (c) 2011-2019 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package io.vertx.kafka.client.consumer.impl;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.internal.ContextInternal;
import io.vertx.kafka.client.common.impl.CloseHandler;
import io.vertx.kafka.client.consumer.AcknowledgeType;
import io.vertx.kafka.client.consumer.KafkaShareConsumer;
import io.vertx.kafka.client.consumer.KafkaShareConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaShareConsumerRecords;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class KafkaShareConsumerImpl<K, V> implements KafkaShareConsumer<K, V> {

  private final KafkaShareReadStreamImpl<K, V> stream;
  private final CloseHandler closeHandler;

  public KafkaShareConsumerImpl(KafkaShareReadStreamImpl<K, V> stream) {
    this.stream = stream;
    this.closeHandler = new CloseHandler((timeout, ar) -> stream.close().onComplete(ar));
  }

  public synchronized KafkaShareConsumerImpl<K, V> registerCloseHook() {
    Context context = Vertx.currentContext();
    if (context == null) {
      return this;
    }
    closeHandler.registerCloseHook((ContextInternal) context);
    return this;
  }

  @Override
  public KafkaShareConsumer<K, V> exceptionHandler(Handler<Throwable> handler) {
    stream.exceptionHandler(handler);
    return this;
  }

  @Override
  public KafkaShareConsumer<K, V> handler(Handler<KafkaShareConsumerRecord<K, V>> handler) {
    if (handler != null) {
      stream.handler(record -> handler.handle(new KafkaShareConsumerRecordImpl<>(record)));
    } else {
      stream.handler(null);
    }
    return this;
  }

  @Override
  public KafkaShareConsumer<K, V> batchHandler(Handler<KafkaShareConsumerRecords<K, V>> handler) {
    if (handler != null) {
      stream.batchHandler(records -> handler.handle(new KafkaShareConsumerRecordsImpl<>(records)));
    } else {
      stream.batchHandler(null);
    }
    return this;
  }

  @Override
  public KafkaShareConsumer<K, V> pause() {
    stream.pause();
    return this;
  }

  @Override
  public KafkaShareConsumer<K, V> resume() {
    stream.resume();
    return this;
  }

  @Override
  public KafkaShareConsumer<K, V> fetch(long amount) {
    stream.fetch(amount);
    return this;
  }

  @Override
  public KafkaShareConsumer<K, V> endHandler(@Nullable Handler<Void> endHandler) {
    stream.endHandler(endHandler);
    return this;
  }

  @Override
  public Future<Void> close() {
    Promise<Void> promise = Promise.promise();
    closeHandler.close(promise);
    return promise.future();
  }

  @Override
  public Future<Void> subscribe(String topic) {
    return stream.subscribe(Set.of(topic));
  }

  @Override
  public Future<Void> subscribe(Set<String> topics) {
    return stream.subscribe(topics);
  }

  @Override
  public Future<Set<String>> subscription() {
    return stream.subscription();
  }

  @Override
  public Future<Void> unsubscribe() {
    return stream.unsubscribe();
  }

  @Override
  public Future<KafkaShareConsumerRecords<K, V>> poll(Duration timeout) {
    return stream.poll(timeout).map(KafkaShareConsumerRecordsImpl::new);
  }

  @Override
  public KafkaShareConsumer<K, V> commitAsync() {
    stream.commitAsync();
    return this;
  }

  @Override
  public KafkaShareConsumer<K, V> setAcknowledgementCommitCallback(AcknowledgementCommitCallback callback) {
    stream.setAcknowledgementCommitCallback(callback);
    return this;
  }

  @Override
  public Future<Void> acknowledge(KafkaShareConsumerRecord<K, V> record, AcknowledgeType type) {
    return stream.acknowledge(record.record(), toKafkaAcknowledgeType(type));
  }

  /**
   * Map the Vert.x acknowledgement type onto the native Kafka one. The switch is exhaustive
   * so that adding a constant to {@link AcknowledgeType} fails at compile time.
   */
  private static org.apache.kafka.clients.consumer.AcknowledgeType toKafkaAcknowledgeType(AcknowledgeType type) {
    switch (type) {
      case ACCEPT:
        return org.apache.kafka.clients.consumer.AcknowledgeType.ACCEPT;
      case RELEASE:
        return org.apache.kafka.clients.consumer.AcknowledgeType.RELEASE;
      case REJECT:
        return org.apache.kafka.clients.consumer.AcknowledgeType.REJECT;
      case RENEW:
        return org.apache.kafka.clients.consumer.AcknowledgeType.RENEW;
      default:
        throw new IllegalArgumentException("Unsupported acknowledge type: " + type);
    }
  }

  @Override
  public Future<Void> commitSync() {
    return stream.commitSync();
  }

  @Override
  public Future<Map<TopicIdPartition, Optional<KafkaException>>> commitSync(Duration timeout) {
    return stream.commitSync(timeout);
  }

  @Override
  public KafkaShareConsumer<K, V> pollTimeout(Duration timeout) {
    stream.setPollTimeout(timeout);
    return this;
  }

  @Override
  public ShareConsumer<K, V> unwrap() {
    return stream.unwrap();
  }
}

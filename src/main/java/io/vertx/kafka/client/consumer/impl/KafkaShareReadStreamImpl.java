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

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.KafkaClientOptions;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaShareReadStreamImpl<K, V> extends AbstractKafkaReadStreamImpl<K, V> {

  private final ShareConsumer<K, V> shareConsumer;
  private AcknowledgementCommitCallback pendingAckCallback;

  public KafkaShareReadStreamImpl(Vertx vertx, ShareConsumer<K, V> shareConsumer) {
    this(vertx, shareConsumer, new KafkaClientOptions(), null);
  }

  public KafkaShareReadStreamImpl(Vertx vertx, ShareConsumer<K, V> shareConsumer, KafkaClientOptions options) {
    this(vertx, shareConsumer, options, null);
  }

  public KafkaShareReadStreamImpl(Vertx vertx, ShareConsumer<K, V> shareConsumer, KafkaClientOptions options, ThreadFactory threadFactory) {
    super(vertx, shareConsumer::wakeup, shareConsumer::close, options, threadFactory);
    this.shareConsumer = shareConsumer;
  }

  @Override
  protected void pollRecords(Handler<ConsumerRecords<K, V>> handler) {
    if (polling.compareAndSet(false, true)) {
      worker.submit(() -> {
        boolean submitted = false;
        try {
          if (!closed.get()) {
            ConsumerRecords<K, V> records = shareConsumer.poll(pollTimeout);
            if (records != null && records.count() > 0) {
              submitted = true;
              context.runOnContext(v -> {
                polling.set(false);
                handler.handle(records);
              });
            }
          }
        } catch (WakeupException ignore) {
        } catch (Exception e) {
          submitted = true;
          final Handler<Throwable> eh = exceptionHandler;
          context.runOnContext(v -> {
            polling.set(false);
            if (eh != null) eh.handle(e);
          });
        } finally {
          if (!submitted) {
            context.runOnContext(v -> {
              polling.set(false);
              handler.handle(ConsumerRecords.empty());
            });
          }
        }
      });
    }
  }

  public Future<Void> subscribe(Set<String> topics) {
    Promise<Void> promise = Promise.promise();

    if (closed.compareAndSet(true, false)) {
      worker = createWorker("vert.x-kafka-share-consumer-thread-");
    }

    AcknowledgementCommitCallback cb = pendingAckCallback;
    pendingAckCallback = null;
    worker.submit(() -> {
      try {
        if (cb != null) shareConsumer.setAcknowledgementCommitCallback(cb);
        shareConsumer.subscribe(topics);
        context.runOnContext(v -> {
          promise.complete();
          schedule();
        });
      } catch (Exception e) {
        context.runOnContext(v -> promise.fail(e));
      }
    });

    return promise.future();
  }

  public Future<Set<String>> subscription() {
    Promise<Set<String>> promise = Promise.promise();
    worker.submit(() -> {
      try {
        Set<String> topics = shareConsumer.subscription();
        context.runOnContext(v -> promise.complete(topics));
      } catch (Exception e) {
        context.runOnContext(v -> promise.fail(e));
      }
    });
    return promise.future();
  }

  public Future<Void> unsubscribe() {
    Promise<Void> promise = Promise.promise();
    worker.submit(() -> {
      try {
        shareConsumer.unsubscribe();
        context.runOnContext(v -> promise.complete());
      } catch (Exception e) {
        context.runOnContext(v -> promise.fail(e));
      }
    });
    return promise.future();
  }

  public Future<ConsumerRecords<K, V>> poll(Duration timeout) {
    Promise<ConsumerRecords<K, V>> promise = Promise.promise();
    if (worker == null) {
      promise.fail(new IllegalStateException("Consumer is not subscribed to any topics"));
      return promise.future();
    }
    worker.submit(() -> {
      try {
        ConsumerRecords<K, V> records = shareConsumer.poll(timeout);
        context.runOnContext(v -> promise.complete(records));
      } catch (WakeupException ignore) {
        context.runOnContext(v -> promise.complete(ConsumerRecords.empty()));
      } catch (Exception e) {
        context.runOnContext(v -> promise.fail(e));
      }
    });
    return promise.future();
  }

  public Future<Void> acknowledge(ConsumerRecord<K, V> record, AcknowledgeType type) {
    Promise<Void> promise = Promise.promise();
    worker.submit(() -> {
      try {
        shareConsumer.acknowledge(record, type);
        context.runOnContext(v -> promise.complete());
      } catch (Exception e) {
        context.runOnContext(v -> promise.fail(e));
      }
    });
    return promise.future();
  }

  public Future<Map<TopicIdPartition, Optional<KafkaException>>> commitSync(Duration timeout) {
    Promise<Map<TopicIdPartition, Optional<KafkaException>>> promise = Promise.promise();
    worker.submit(() -> {
      try {
        Map<TopicIdPartition, Optional<KafkaException>> result = timeout != null
          ? shareConsumer.commitSync(timeout)
          : shareConsumer.commitSync();
        context.runOnContext(v -> promise.complete(result));
      } catch (Exception e) {
        context.runOnContext(v -> promise.fail(e));
      }
    });
    return promise.future();
  }

  public Future<Void> commitSync() {
    return commitSync(null).flatMap(map -> {
      List<Map.Entry<TopicIdPartition, KafkaException>> errors = map.entrySet().stream()
        .filter(e -> e.getValue().isPresent())
        .map(e -> Map.entry(e.getKey(), e.getValue().get()))
        .collect(Collectors.toList());

      if (errors.isEmpty()) return Future.succeededFuture();
      if (errors.size() == 1) return Future.failedFuture(errors.get(0).getValue());

      String message = errors.stream()
        .map(e ->
          "[" + e.getKey().topic() + "-" + e.getKey().partition() + ": " + e.getValue().getMessage() + "]"
        ).collect(Collectors.joining("\n"));
      return Future.failedFuture(new KafkaException(errors.size() + " partition(s) failed to commit acknowledgements:\n" + message));
    });
  }

  public void commitAsync() {
    worker.submit(() -> {
      try {
        shareConsumer.commitAsync();
      } catch (Exception e) {
        if (exceptionHandler != null) {
          context.runOnContext(v -> exceptionHandler.handle(e));
        }
      }
    });
  }

  public void setAcknowledgementCommitCallback(AcknowledgementCommitCallback callback) {
    AcknowledgementCommitCallback wrapped = callback == null ? null :
      (offsets, exception) -> context.runOnContext(v -> callback.onComplete(offsets, exception));
    if (worker == null) {
      pendingAckCallback = wrapped;
    } else {
      worker.submit(() -> shareConsumer.setAcknowledgementCommitCallback(wrapped));
    }
  }

  public void setPollTimeout(Duration timeout) {
    this.pollTimeout = timeout;
  }

  public ShareConsumer<K, V> unwrap() {
    return shareConsumer;
  }
}

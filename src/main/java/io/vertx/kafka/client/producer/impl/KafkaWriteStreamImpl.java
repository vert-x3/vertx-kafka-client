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

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.EventExecutor;
import io.vertx.core.internal.VertxInternal;
import io.vertx.kafka.client.common.KafkaClientOptions;
import io.vertx.kafka.client.common.tracing.ProducerTracer;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

/**
 * Kafka write stream implementation
 */
public class KafkaWriteStreamImpl<K, V> implements KafkaWriteStream<K, V> {

  private long maxSize = DEFAULT_MAX_SIZE;
  private long pending;
  private final Producer<K, V> producer;
  private Handler<Void> drainHandler;
  private Handler<Throwable> exceptionHandler;
  private final VertxInternal vertx;
  private final ProducerTracer tracer;
  private final Executor workerExec;

  public KafkaWriteStreamImpl(Vertx vertx, Producer<K, V> producer, KafkaClientOptions options) {
    ContextInternal ctxInt = ((ContextInternal) vertx.getOrCreateContext()).unwrap();
    this.producer = producer;
    this.vertx = (VertxInternal) vertx;
    this.tracer = ProducerTracer.create(ctxInt.tracer(), options);
    this.workerExec = ((VertxInternal)vertx).createWorkerContext().executor();
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
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    ContextInternal ctx = vertx.getOrCreateContext();
    ProducerTracer.StartedSpan startedSpan = this.tracer == null ? null : this.tracer.prepareSendMessage(ctx, record);
    int len = this.len(record.value());
    this.pending += len;
    Promise<RecordMetadata> prom = ctx.promise();
    workerExec.execute(() -> {
      try {
        this.producer.send(record, (metadata, err) -> {

          // callback from Kafka IO thread
          ctx.runOnContext(v1 -> {
            Handler<Throwable> exceptionHandler;
            Handler<Void> drainHandler;
            synchronized (KafkaWriteStreamImpl.this) {
              exceptionHandler = this.exceptionHandler;
              long lowWaterMark = this.maxSize / 2;
              this.pending -= len;
              if (this.pending < lowWaterMark && this.drainHandler != null) {
                drainHandler = this.drainHandler;
                this.drainHandler = null;
              } else {
                drainHandler = null;
              }
            }
            // if exception happens, no record written
            if (err != null) {
              if (exceptionHandler != null) {
                ctx.runOnContext(v2 -> exceptionHandler.handle(err));
              } else {
                ctx.reportException(err);
              }
            }
            if (drainHandler != null) {
              ctx.runOnContext(drainHandler);
            }
          });

          if (err != null) {
            if (startedSpan != null) {
              startedSpan.fail(ctx, err);
            }
            prom.fail(err);
          } else {
            if (startedSpan != null) {
              startedSpan.finish(ctx);
            }
            prom.complete(metadata);
          }
        });
      } catch (Throwable e) {
        synchronized (KafkaWriteStreamImpl.this) {
          if (this.exceptionHandler != null) {
            Handler<Throwable> exceptionHandler = this.exceptionHandler;
            ctx.runOnContext(v3 -> exceptionHandler.handle(e));
          }
        }
        if (startedSpan != null) {
          startedSpan.fail(ctx, e);
        }
        prom.fail(e);
      }
    });
    return prom.future();
  }

  @Override
  public Future<Void> write(ProducerRecord<K, V> record) {
    return this.send(record).mapEmpty();
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
  public Future<Void> end() {
    Promise<Void> promise = Promise.promise();
    vertx.runOnContext(v -> promise.complete());
    return promise.future();
  }

  @Override
  public Future<Void> initTransactions() {
    return executeBlocking(this.producer::initTransactions);
  }

  @Override
  public Future<Void> beginTransaction() {
    return executeBlocking(this.producer::beginTransaction);
  }

  @Override
  public Future<Void> commitTransaction() {
    return executeBlocking(this.producer::commitTransaction);
  }

  @Override
  public Future<Void> abortTransaction() {
    return executeBlocking(this.producer::abortTransaction);
  }

  @Override
  public KafkaWriteStreamImpl<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.exceptionHandler = handler;
    return this;
  }

  @Override
  public Future<List<PartitionInfo>> partitionsFor(String topic) {
    return execute(() -> this.producer.partitionsFor(topic));
  }

  @Override
  public Future<Void> flush() {
    return execute(() -> {
      this.producer.flush();
      return null;
    });
  }

  @Override
  public Future<Void> close() {
    return close(0);
  }

  @Override
  public Future<Void> close(long timeout) {
    return execute(() -> {
      if (timeout > 0) {
        this.producer.close(Duration.ofMillis(timeout));
      } else {
        this.producer.close();
      }
      return null;
    });
  }

  private <T> Future<T> execute(Callable<T> callable) {
    ContextInternal ctx = vertx.getOrCreateContext();
    return ctx.future(promise -> {
      workerExec.execute(() -> {
        try {
          T res = callable.call();
          promise.complete(res);
        } catch (Exception e) {
          promise.fail(e);
        }
      });
    });
  }

  @Override
  public Producer<K, V> unwrap() {
    return this.producer;
  }

  Future<Void> executeBlocking(final BlockingStatement statement) {
    return execute(() -> {
      statement.execute();
      return null;
    });
  }

  @FunctionalInterface
  private interface BlockingStatement {

    void execute();
  }
}

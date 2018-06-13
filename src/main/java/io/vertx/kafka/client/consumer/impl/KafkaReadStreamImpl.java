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

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.kafka.client.common.impl.Helper;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * Kafka read stream implementation
 */
public class KafkaReadStreamImpl<K, V> implements KafkaReadStream<K, V> {

  private static final AtomicInteger threadCount = new AtomicInteger(0);

  private final Context context;
  private final AtomicBoolean closed = new AtomicBoolean(true);
  private final Consumer<K, V> consumer;

  private final AtomicBoolean consuming = new AtomicBoolean(false);
  private final AtomicBoolean paused = new AtomicBoolean(false);
  private Handler<ConsumerRecord<K, V>> recordHandler;
  private Handler<Throwable> exceptionHandler;
  private Iterator<ConsumerRecord<K, V>> current; // Accessed on event loop
  private Handler<ConsumerRecords<K, V>> batchHandler;
  private Handler<Set<TopicPartition>> partitionsRevokedHandler;
  private Handler<Set<TopicPartition>> partitionsAssignedHandler;
  private long pollTimeout = 1000L;

  private ExecutorService worker;

  private final ConsumerRebalanceListener rebalanceListener =  new ConsumerRebalanceListener() {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

      Handler<Set<TopicPartition>> handler = partitionsRevokedHandler;
      if (handler != null) {
        context.runOnContext(v -> {
          handler.handle(Helper.toSet(partitions));
        });
      }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

      Handler<Set<TopicPartition>> handler = partitionsAssignedHandler;
      if (handler != null) {
        context.runOnContext(v -> {
          handler.handle(Helper.toSet(partitions));
        });
      }
    }
  };

  public KafkaReadStreamImpl(Context context, Consumer<K, V> consumer) {
    this.context = context;
    this.consumer = consumer;
  }

  private <T> void start(java.util.function.BiConsumer<Consumer<K, V>, Future<T>> task, Handler<AsyncResult<T>> handler) {
    this.worker = Executors.newSingleThreadExecutor(r -> new Thread(r, "vert.x-kafka-consumer-thread-" + threadCount.getAndIncrement()));
    this.submitTaskWhenStarted(task, handler);
  }

  private <T> void submitTaskWhenStarted(java.util.function.BiConsumer<Consumer<K, V>, Future<T>> task, Handler<AsyncResult<T>> handler) {
    if (worker == null) {
      throw new IllegalStateException();
    }
    this.worker.submit(() -> {
      Future<T> future;
      if (handler != null) {
        future = Future.future();
        future.setHandler(event-> {
          // When we've executed the task on the worker thread,
          // run the callback on the eventloop thread
          this.context.runOnContext(v-> {
            handler.handle(event);
            });
          });

      } else {
        future = null;
      }
      try {
        task.accept(this.consumer, future);
      } catch (Exception e) {
        if (future != null && !future.isComplete()) {
          future.fail(e);
        }
      }
    });
  }

  private void pollRecords(Handler<ConsumerRecords<K, V>> handler) {
    this.worker.submit(() -> {
      if (!this.closed.get()) {
        try {
          ConsumerRecords<K, V> records = this.consumer.poll(pollTimeout);
          if (records != null && records.count() > 0) {
            this.context.runOnContext(v -> handler.handle(records));
          } else {
            // Don't call pollRecords directly, but use schedule() to actually pause when the readStream is paused
            schedule(0);
          }
        } catch (WakeupException ignore) {
        } catch (Exception e) {
          if (exceptionHandler != null) {
            exceptionHandler.handle(e);
          }
        }
      }
    });
  }

  private void schedule(long delay) {
    if (this.consuming.get()
        && !this.paused.get()
        && this.recordHandler != null) {

      Handler<ConsumerRecord<K, V>> handler = this.recordHandler;
      this.context.runOnContext(v1 -> {
        if (delay > 0) {
          this.context.owner().setTimer(delay, v2 -> run(handler));
        } else {
          run(handler);
        }
      });
    }
  }

  // Access the consumer from the event loop since the consumer is not thread safe
  private void run(Handler<ConsumerRecord<K, V>> handler) {

    if (this.closed.get()) {
      return;
    }

    if (this.current == null || !this.current.hasNext()) {

      this.pollRecords(records -> {

        if (records != null && records.count() > 0) {
          this.current = records.iterator();
          if (batchHandler != null) {
            batchHandler.handle(records);
          }
          this.schedule(0);
        } else {
          this.schedule(1);
        }
      });

    } else {

      int count = 0;
      while (this.current.hasNext() && count++ < 10) {

        // to honor the Vert.x ReadStream contract, handler should not be called if stream is paused
        if (this.paused.get())
          break;

        ConsumerRecord<K, V> next = this.current.next();
        if (handler != null) {
          handler.handle(next);
        }
      }
      this.schedule(0);
    }
  }

  protected <T> void submitTask(java.util.function.BiConsumer<Consumer<K, V>, Future<T>> task,
      Handler<AsyncResult<T>> handler) {
    if (this.closed.compareAndSet(true, false)) {
      this.start(task, handler);
    } else {
      this.submitTaskWhenStarted(task, handler);
    }
  }

  @Override
  public KafkaReadStream<K, V> pause(Set<TopicPartition> topicPartitions) {
    return pause(topicPartitions, null);
  }

  @Override
  public KafkaReadStream<K, V> pause(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {

    this.submitTask((consumer, future) -> {
      consumer.pause(topicPartitions);
      if (future != null) {
        future.complete();
      }
    }, completionHandler);

    return this;
  }

  @Override
  public void paused(Handler<AsyncResult<Set<TopicPartition>>> handler) {

    this.submitTask((consumer, future) -> {
      Set<TopicPartition> result = consumer.paused();
      if (future != null) {
        future.complete(result);
      }
    }, handler);
  }

  @Override
  public KafkaReadStream<K, V> resume(Set<TopicPartition> topicPartitions) {
    return this.resume(topicPartitions, null);
  }

  @Override
  public KafkaReadStream<K, V> resume(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {

    this.submitTask((consumer, future) -> {
      consumer.resume(topicPartitions);
      if (future != null) {
        future.complete();
      }
    }, completionHandler);

    return this;
  }

  @Override
  public void committed(TopicPartition topicPartition, Handler<AsyncResult<OffsetAndMetadata>> handler) {

    this.submitTask((consumer, future) -> {
      OffsetAndMetadata result = consumer.committed(topicPartition);
      if (future != null) {
        future.complete(result);
      }
    }, handler);
  }

  @Override
  public KafkaReadStream<K, V> seekToEnd(Set<TopicPartition> topicPartitions) {
    return this.seekToEnd(topicPartitions, null);
  }

  @Override
  public KafkaReadStream<K, V> seekToEnd(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
    this.context.runOnContext(r -> {
      current = null;

      this.submitTask((consumer, future) -> {
        consumer.seekToEnd(topicPartitions);
        if (future != null) {
          future.complete();
        }
      }, completionHandler);
    });
    return this;
  }

  @Override
  public KafkaReadStream<K, V> seekToBeginning(Set<TopicPartition> topicPartitions) {
    return this.seekToBeginning(topicPartitions, null);
  }

  @Override
  public KafkaReadStream<K, V> seekToBeginning(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
    this.context.runOnContext(r -> {
      current = null;

      this.submitTask((consumer, future) -> {
        consumer.seekToBeginning(topicPartitions);
        if (future != null) {
          future.complete();
        }
      }, completionHandler);
    });
    return this;
  }

  @Override
  public KafkaReadStream<K, V> seek(TopicPartition topicPartition, long offset) {
    return this.seek(topicPartition, offset, null);
  }

  @Override
  public KafkaReadStream<K, V> seek(TopicPartition topicPartition, long offset, Handler<AsyncResult<Void>> completionHandler) {
    this.context.runOnContext(r -> {
      current = null;

      this.submitTask((consumer, future) -> {
        consumer.seek(topicPartition, offset);
        if (future != null) {
          future.complete();
        }
      }, completionHandler);
    });

    return this;
  }

  @Override
  public KafkaReadStream<K, V> partitionsRevokedHandler(Handler<Set<TopicPartition>> handler) {
    this.partitionsRevokedHandler = handler;
    return this;
  }

  @Override
  public KafkaReadStream<K, V> partitionsAssignedHandler(Handler<Set<TopicPartition>> handler) {
    this.partitionsAssignedHandler = handler;
    return this;
  }

  @Override
  public KafkaReadStream<K, V> subscribe(Set<String> topics) {
    return subscribe(topics, null);
  }

  @Override
  public KafkaReadStream<K, V> subscribe(Set<String> topics, Handler<AsyncResult<Void>> completionHandler) {

    BiConsumer<Consumer<K, V>, Future<Void>> handler = (consumer, future) -> {
      consumer.subscribe(topics, this.rebalanceListener);
      this.startConsuming();
      if (future != null) {
        future.complete();
      }
    };

    if (this.closed.compareAndSet(true, false)) {
      this.start(handler, completionHandler);
    } else {
      this.submitTask(handler, completionHandler);
    }

    return this;
  }

  @Override
  public KafkaReadStream<K, V> unsubscribe() {
    return this.unsubscribe(null);
  }

  @Override
  public KafkaReadStream<K, V> unsubscribe(Handler<AsyncResult<Void>> completionHandler) {

    this.submitTask((consumer, future) -> {
      consumer.unsubscribe();
      if (future != null) {
        future.complete();
      }
    }, completionHandler);

    return this;
  }

  @Override
  public KafkaReadStream<K, V> subscription(Handler<AsyncResult<Set<String>>> handler) {

    this.submitTask((consumer, future) -> {
      Set<String> subscription = consumer.subscription();
      if (future != null) {
        future.complete(subscription);
      }
    }, handler);

    return this;
  }

  @Override
  public KafkaReadStream<K, V> assign(Set<TopicPartition> partitions) {
    return this.assign(partitions, null);
  }

  @Override
  public KafkaReadStream<K, V> assign(Set<TopicPartition> partitions, Handler<AsyncResult<Void>> completionHandler) {

    BiConsumer<Consumer<K, V>, Future<Void>> handler = (consumer, future) -> {
      consumer.assign(partitions);
      this.startConsuming();
      if (future != null) {
        future.complete();
      }
    };

    if (this.closed.compareAndSet(true, false)) {
      this.start(handler, completionHandler);
    } else {
      this.submitTask(handler, completionHandler);
    }

    return this;
  }

  @Override
  public KafkaReadStream<K, V> assignment(Handler<AsyncResult<Set<TopicPartition>>> handler) {

    this.submitTask((consumer, future) -> {
      Set<TopicPartition> partitions = consumer.assignment();
      if (future != null) {
        future.complete(partitions);
      }
    }, handler);

    return this;
  }

  @Override
  public KafkaReadStream<K, V> listTopics(Handler<AsyncResult<Map<String,List<PartitionInfo>>>> handler) {

    this.submitTask((consumer, future) -> {
      Map<String, List<PartitionInfo>> topics = consumer.listTopics();
      if (future != null) {
        future.complete(topics);
      }
    }, handler);

    return this;
  }

  @Override
  public void commit() {
    this.commit((Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>>) null);
  }

  @Override
  public void commit(Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler) {
    this.commit(null, completionHandler);
  }

  @Override
  public void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    this.commit(offsets, null);
  }

  @Override
  public void commit(Map<TopicPartition, OffsetAndMetadata> offsets, Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler) {
    this.submitTask((consumer, future) -> {
      OffsetCommitCallback callback = (result, exception) -> {
        if (future != null) {
          if (exception != null) {
            future.fail(exception);
          } else {
            future.complete(result);
          }
        }
      };
      if (offsets == null) {
        consumer.commitAsync(callback);
      } else {
        consumer.commitAsync(offsets, callback);
      }
    }, completionHandler);
  }

  @Override
  public KafkaReadStreamImpl<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler) {

    this.submitTask((consumer, future) -> {
      List<PartitionInfo> partitions = consumer.partitionsFor(topic);
      if (future != null) {
        future.complete(partitions);
      }
    }, handler);

    return this;
  }

  @Override
  public KafkaReadStreamImpl<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.exceptionHandler = handler;
    return this;
  }

  @Override
  public KafkaReadStreamImpl<K, V> handler(Handler<ConsumerRecord<K, V>> handler) {
    this.recordHandler = handler;
    this.schedule(0);
    return this;
  }

  @Override
  public KafkaReadStreamImpl<K, V> pause() {
    this.paused.set(true);
    return this;
  }

  @Override
  public KafkaReadStreamImpl<K, V> resume() {
    if (this.paused.compareAndSet(true, false)) {
      this.schedule(0);
    }
    return this;
  }

  private KafkaReadStreamImpl<K, V> startConsuming() {
    this.consuming.set(true);
    this.schedule(0);
    return this;
  }

  @Override
  public KafkaReadStreamImpl<K, V> endHandler(Handler<Void> endHandler) {
    return this;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> completionHandler) {
    if (this.closed.compareAndSet(false, true)) {
      this.worker.submit(() -> {
        this.consumer.close();
        this.context.runOnContext(v -> {
          this.worker.shutdownNow();
          if (completionHandler != null) {
            completionHandler.handle(Future.succeededFuture());
          }
        });
      });
      this.consumer.wakeup();
    }
    else {
      if (completionHandler != null) {
        completionHandler.handle(Future.succeededFuture());
      }
    }
  }

  @Override
  public void position(TopicPartition partition, Handler<AsyncResult<Long>> handler) {
    this.submitTask((consumer, future) -> {
      long pos = this.consumer.position(partition);
      if (future != null) {
        future.complete(pos);
      }
    }, handler);
  }

  @Override
  public void offsetsForTimes(Map<TopicPartition, Long> topicPartitionTimestamps, Handler<AsyncResult<Map<TopicPartition, OffsetAndTimestamp>>> handler) {
    this.submitTask((consumer, future) -> {
      Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = this.consumer.offsetsForTimes(topicPartitionTimestamps);
      if (future != null) {
        future.complete(offsetsForTimes);
      }
    }, handler);
  }

  @Override
  public void offsetsForTimes(TopicPartition topicPartition, long timestamp, Handler<AsyncResult<OffsetAndTimestamp>> handler) {
    this.submitTask((consumer, future) -> {
      Map<TopicPartition, Long> input = new HashMap<>();
      input.put(topicPartition, timestamp);

      Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = this.consumer.offsetsForTimes(input);
      if (future != null) {
        future.complete(offsetsForTimes.get(topicPartition));
      }
    }, handler);
  }


  @Override
  public void beginningOffsets(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Map<TopicPartition, Long>>> handler) {
    this.submitTask((consumer, future) -> {
      Map<TopicPartition, Long> beginningOffsets = this.consumer.beginningOffsets(topicPartitions);
      if (future != null) {
        future.complete(beginningOffsets);
      }
    }, handler);
  }

  @Override
  public void beginningOffsets(TopicPartition topicPartition, Handler<AsyncResult<Long>> handler) {
    this.submitTask((consumer, future) -> {
      Set<TopicPartition> input = new HashSet<>();
      input.add(topicPartition);
      Map<TopicPartition, Long> beginningOffsets = this.consumer.beginningOffsets(input);
      if (future != null) {
        future.complete(beginningOffsets.get(topicPartition));
      }
    }, handler);
  }

  @Override
  public void endOffsets(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Map<TopicPartition, Long>>> handler) {
    this.submitTask((consumer, future) -> {
      Map<TopicPartition, Long> endOffsets = this.consumer.endOffsets(topicPartitions);
      if (future != null) {
        future.complete(endOffsets);
      }
    }, handler);
  }

  @Override
  public void endOffsets(TopicPartition topicPartition, Handler<AsyncResult<Long>> handler) {
    this.submitTask((consumer, future) -> {
      Set<TopicPartition> input = new HashSet<>();
      input.add(topicPartition);
      Map<TopicPartition, Long> endOffsets = this.consumer.endOffsets(input);
      if (future != null) {
        future.complete(endOffsets.get(topicPartition));
      }
    }, handler);
  }

  @Override
  public Consumer<K, V> unwrap() {
    return this.consumer;
  }

  public KafkaReadStream batchHandler(Handler<ConsumerRecords<K, V>> handler) {
    this.batchHandler = handler;
    return this;
  }

  @Override
  public KafkaReadStream<K, V> pollTimeout(long timeout) {
    this.pollTimeout = timeout;
    return this;
  }

  @Override
  public void poll(long timeout, Handler<AsyncResult<ConsumerRecords<K, V>>> handler) {
    this.worker.submit(() -> {
      if (!this.closed.get()) {
        Future fut;
        try {
          ConsumerRecords<K, V> records = this.consumer.poll(timeout);
          this.context.runOnContext(v -> handler.handle(Future.succeededFuture(records)));
        } catch (WakeupException ignore) {
          this.context.runOnContext(v -> handler.handle(Future.succeededFuture(ConsumerRecords.empty())));
        } catch (Exception e) {
          this.context.runOnContext(v -> handler.handle(Future.failedFuture(e)));
        }
      }
    });
  }
}

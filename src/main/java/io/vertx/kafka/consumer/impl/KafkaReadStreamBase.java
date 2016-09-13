package io.vertx.kafka.consumer.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.kafka.consumer.KafkaReadStream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
abstract class KafkaReadStreamBase<K, V> implements KafkaReadStream<K, V> {

  final Context context;
  final AtomicBoolean closed = new AtomicBoolean(true);

  private final AtomicBoolean paused = new AtomicBoolean(true);
  private Handler<ConsumerRecord<K, V>> recordHandler;
  private Iterator<ConsumerRecord<K, V>> current; // Accessed on event loop
  private Handler<Collection<TopicPartition>> partitionsRevokedHandler;
  private Handler<Collection<TopicPartition>> partitionsAssignedHandler;

  private final ConsumerRebalanceListener rebalanceListener =  new ConsumerRebalanceListener() {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      Handler<Collection<TopicPartition>> handler = partitionsRevokedHandler;
      if (handler != null) {
        context.runOnContext(v -> {
          handler.handle(partitions);
        });
      }
    }
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      Handler<Collection<TopicPartition>> handler = partitionsAssignedHandler;
      if (handler != null) {
        context.runOnContext(v -> {
          handler.handle(partitions);
        });
      }
    }
  };

  KafkaReadStreamBase(Context context) {
    this.context = context;
  }

  private void schedule(long delay) {
    if (!paused.get()) {
      Handler<ConsumerRecord<K, V>> handler = recordHandler;
      if (delay > 0) {
        context.owner().setTimer(delay, v -> run(handler));
      } else {
        context.runOnContext(v -> run(handler));
      }
    }
  }

  protected abstract <T> void start(java.util.function.BiConsumer<Consumer, Future<T>> task, Handler<AsyncResult<T>> handler);

  protected abstract <T> void executeTask(java.util.function.BiConsumer<Consumer, Future<T>> task, Handler<AsyncResult<T>> handler);

  protected abstract void poll(Handler<ConsumerRecords<K, V>> handler);

  // Access the consumer from the event loop since the consumer is not thread safe
  private void run(Handler<ConsumerRecord<K, V>> handler) {
    if (closed.get()) {
      return;
    }
    if (current == null || !current.hasNext()) {
      poll(records -> {
        if (records != null && records.count() > 0) {
          current = records.iterator();
          schedule(0);
        } else {
          schedule(1);
        }
      });
    } else {
      int count = 0;
      while (current.hasNext() && count++ < 10) {
        ConsumerRecord<K, V> next = current.next();
        if (handler != null) {
          handler.handle(next);
        }
      }
      schedule(0);
    }
  }

  @Override
  public void commited(TopicPartition topicPartition, Handler<AsyncResult<OffsetAndMetadata>> handler) {
    executeTask((cons, fut) -> {
      OffsetAndMetadata result = cons.committed(topicPartition);
      if (fut != null) {
        fut.complete(result);
      }
    }, handler);
  }

  @Override
  public KafkaReadStream<K, V> seek(TopicPartition topicPartition, long offset) {
    return seek(topicPartition, offset, null);
  }

  @Override
  public KafkaReadStream<K, V> seek(TopicPartition topicPartition, long offset, Handler<AsyncResult<Void>> completionHandler) {
    executeTask((cons, fut) -> {
      cons.seek(topicPartition, offset);
      if (fut != null) {
        fut.complete();
      }
    }, completionHandler);
    return this;
  }

  @Override
  public KafkaReadStream<K, V> partitionsRevokedHandler(Handler<Collection<TopicPartition>> handler) {
    partitionsRevokedHandler = handler;
    return this;
  }

  @Override
  public KafkaReadStream<K, V> partitionsAssignedHandler(Handler<Collection<TopicPartition>> handler) {
    partitionsAssignedHandler = handler;
    return this;
  }

  @Override
  public KafkaReadStream<K, V> subscribe(Set<String> topics) {
    return subscribe(topics, null);
  }

  @Override
  public KafkaReadStream<K, V> subscribe(Set<String> topics, Handler<AsyncResult<Void>> handler) {
    if (recordHandler == null) {
      throw new IllegalStateException();
    }
    if (closed.compareAndSet(true, false)) {
      start((cons, fut) -> {
        cons.subscribe(topics, rebalanceListener);
        resume();
        if (fut != null) {
          fut.complete();
        }
      }, handler);
    } else {
      executeTask((cons, fut) -> {
        cons.subscribe(topics, rebalanceListener);
        if (fut != null) {
          fut.complete();
        }
      }, handler);
    }
    return this;
  }

  @Override
  public void commit() {
    commit((Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>>) null);
  }

  @Override
  public void commit(Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler) {
    commit(null, completionHandler);
  }

  @Override
  public void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    commit(offsets, null);
  }

  @Override
  public void commit(Map<TopicPartition, OffsetAndMetadata> offsets, Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler) {
    executeTask((cons, fut) -> {
      OffsetCommitCallback callback = (result, exception) -> {
        if (fut != null) {
          if (exception != null) {
            fut.fail(exception);
          } else {
            fut.complete(result);
          }
        }
      };
      if (offsets == null) {
        cons.commitAsync(callback);
      } else {
        cons.commitAsync(offsets, callback);
      }
    }, completionHandler);
  }

  @Override
  public KafkaReadStreamBase<K, V> exceptionHandler(Handler<Throwable> handler) {
    return this;
  }

  @Override
  public KafkaReadStreamBase<K, V> handler(Handler<ConsumerRecord<K, V>> handler) {
    recordHandler = handler;
    return this;
  }

  @Override
  public KafkaReadStreamBase<K, V> pause() {
    paused.set(true);
    return this;
  }

  @Override
  public KafkaReadStreamBase<K, V> resume() {
    if (paused.compareAndSet(true, false)) {
      schedule(0);
    }
    return this;
  }

  @Override
  public KafkaReadStreamBase<K, V> endHandler(Handler<Void> endHandler) {
    return this;
  }

  @Override
  public void close(Handler<Void> completionHandler) {
    if (closed.compareAndSet(false, true)) {
      doClose(completionHandler);
    }
  }

  protected abstract void doClose(Handler<Void> completionHandler);
}

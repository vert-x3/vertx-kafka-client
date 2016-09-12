package io.vertx.kafka.impl;

import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.kafka.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
abstract class KafkaConsumerBase<K, V> implements KafkaConsumer<K, V> {

  final Context context;
  final AtomicBoolean closed = new AtomicBoolean(true);

  private final AtomicBoolean paused = new AtomicBoolean(true);
  private Handler<ConsumerRecord<K, V>> recordHandler;
  private Iterator<ConsumerRecord<K, V>> current; // Accessed on event loop

  KafkaConsumerBase(Context context) {
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

  protected abstract void start(java.util.function.Consumer<Consumer> task);

  protected abstract void executeTask(java.util.function.Consumer<Consumer> task);

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
  public KafkaConsumer<K, V> subscribe(Set<String> topics) {
    return subscribe(topics, null);
  }

  @Override
  public KafkaConsumer<K, V> subscribe(Set<String> topics, Handler<Void> handler) {
    if (recordHandler == null) {
      throw new IllegalStateException();
    }
    if (closed.compareAndSet(true, false)) {
      start(cons -> {
        cons.subscribe(topics);
        resume();
        if (handler != null) {
          handler.handle(null);
        }
      });
    } else {
      executeTask(cons -> {
        cons.subscribe(topics);
        if (handler != null) {
          handler.handle(null);
        }
      });
    }
    return this;
  }

  @Override
  public KafkaConsumerBase<K, V> exceptionHandler(Handler<Throwable> handler) {
    return this;
  }

  @Override
  public KafkaConsumerBase<K, V> handler(Handler<ConsumerRecord<K, V>> handler) {
    recordHandler = handler;
    return this;
  }

  @Override
  public KafkaConsumerBase<K, V> pause() {
    paused.set(true);
    return this;
  }

  @Override
  public KafkaConsumerBase<K, V> resume() {
    if (paused.compareAndSet(true, false)) {
      schedule(0);
    }
    return this;
  }

  @Override
  public KafkaConsumerBase<K, V> endHandler(Handler<Void> endHandler) {
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

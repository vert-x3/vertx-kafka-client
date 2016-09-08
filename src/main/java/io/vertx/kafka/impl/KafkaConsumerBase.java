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
public abstract class KafkaConsumerBase<K, V> implements KafkaConsumer<K, V> {

  protected final Context context;
  protected final Consumer<K, V> consumer;

  private final AtomicBoolean paused = new AtomicBoolean(true);
  protected final AtomicBoolean closed = new AtomicBoolean(true);
  private Handler<ConsumerRecord<K, V>> recordHandler;

  protected Iterator<ConsumerRecord<K, V>> current; // Accessed on event loop

  protected KafkaConsumerBase(Context context, Consumer<K, V> consumer) {
    this.consumer = consumer;
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

  protected abstract ConsumerRecords<K, V> fetchRecords();

  // Access the consumer from the event loop since the consumer is not thread safe
  private void run(Handler<ConsumerRecord<K, V>> handler) {
    if (closed.get()) {
      return;
    }
    if (current == null || !current.hasNext()) {
      ConsumerRecords<K, V> records = fetchRecords();
      if (records != null && records.count() > 0) {
        current = records.iterator();
      } else {
        schedule(1);
        return;
      }
    }
    int count = 0;
    while (current.hasNext() && count++ < 10) {
      ConsumerRecord<K, V> next = current.next();
      if (handler != null) {
        handler.handle(next);
      }
    }
    schedule(0);
  }

  public KafkaConsumerBase<K, V> subscribe(Set<String> topics) {
    consumer.subscribe(topics);
    return this;
  }

  @Override
  public KafkaConsumerBase<K, V> exceptionHandler(Handler<Throwable> handler) {
    return this;
  }

  @Override
  public KafkaConsumerBase<K, V> handler(Handler<ConsumerRecord<K, V>> handler) {
    recordHandler = handler;
    if (handler != null && closed.compareAndSet(true, false)) {
      start();
    }
    return this;
  }

  protected void start() {
    resume();
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

package io.vertx.kafka;

import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.ReadStream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class KafkaConsumer<K, V> implements ReadStream<ConsumerRecord<K, V>> {

  public static <K, V> KafkaConsumer<K, V> create(Vertx vertx, Map<String, Object> props) {
    return new KafkaConsumer<>(vertx.getOrCreateContext(), props);
  }

  public static <K, V> KafkaConsumer<K, V> create(Vertx vertx, Consumer<K, V> consumer) {
    return new KafkaConsumer<>(vertx.getOrCreateContext(), consumer);
  }

  private final Context context;
  private final Consumer<K, V> consumer;
  private Iterator<ConsumerRecord<K, V>> current; // Accessed on event loop

  private final AtomicBoolean paused = new AtomicBoolean(true);
  private Handler<ConsumerRecord<K, V>> recordHandler;

  private KafkaConsumer(Context context, Map<String, Object> props) {
    this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
    this.context = context;
  }

  private KafkaConsumer(Context context, Consumer<K, V> consumer) {
    this.consumer = consumer;
    this.context = context;
  }

  private void schedule(long delay) {
    if (!paused.get()) {
      Handler<ConsumerRecord<K, V>> handler = recordHandler;
      if (delay > 0) {
        context.owner().setTimer(1, v -> run(handler));
      } else {
        context.runOnContext(v -> run(handler));
      }
    }
  }

  private void run(Handler<ConsumerRecord<K, V>> handler) {
    if (current == null || !current.hasNext()) {
      current = consumer.poll(0).iterator();
    }
    if (current.hasNext()) {
      int count = 0;
      while (current.hasNext() && count++ < 10) {
        ConsumerRecord<K, V> next = current.next();
        if (handler != null) {
          handler.handle(next);
        }
      }
      schedule(0);
    } else {
      schedule(1);
    }
  }

  public KafkaConsumer<K, V> subscribe(Set<String> topics) {
    consumer.subscribe(topics);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> exceptionHandler(Handler<Throwable> handler) {
    return this;
  }

  @Override
  public KafkaConsumer<K, V> handler(Handler<ConsumerRecord<K, V>> handler) {
    recordHandler = handler;
    if (handler != null) {
      resume();
    }
    return this;
  }

  @Override
  public KafkaConsumer<K, V> pause() {
    paused.set(true);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> resume() {
    if (paused.compareAndSet(true, false)) {
      schedule(0);
    }
    return this;
  }

  @Override
  public KafkaConsumer<K, V> endHandler(Handler<Void> endHandler) {
    return this;
  }
}

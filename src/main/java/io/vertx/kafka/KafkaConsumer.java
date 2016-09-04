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
  private Consumer<K, V> consumer;
  private boolean paused;
  private Handler<ConsumerRecord<K, V>> recordHandler;
  private Iterator<ConsumerRecord<K, V>> current;

  private KafkaConsumer(Context context, Map<String, Object> props) {
    this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
    this.context = context;
  }

  private KafkaConsumer(Context context, Consumer<K, V> consumer) {
    this.consumer = consumer;
    this.context = context;
  }

  private void schedule(long delay) {
    if (delay > 0) {
      context.owner().setTimer(1, v -> run());
    } else {
      context.runOnContext(v -> run());
    }
  }

  private synchronized void run() {
    if (!paused) {
      if (current == null || !current.hasNext()) {
        current = consumer.poll(0).iterator();
      }
      if (current.hasNext()) {
        int count = 0;
        while (current.hasNext() && count++ < 10) {
          ConsumerRecord<K, V> next = current.next();
          recordHandler.handle(next);
        }
        schedule(0);
      } else {
        schedule(1);
      }
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
  public synchronized KafkaConsumer<K, V> handler(Handler<ConsumerRecord<K, V>> handler) {
    recordHandler = handler;
    if (handler != null) {
      schedule(0);
    }
    return this;
  }

  @Override
  public synchronized KafkaConsumer<K, V> pause() {
    if (!paused) {
      paused = true;
    }
    return this;
  }

  @Override
  public synchronized KafkaConsumer<K, V> resume() {
    if (paused) {
      paused = false;
      schedule(0);
    }
    return this;
  }

  @Override
  public KafkaConsumer<K, V> endHandler(Handler<Void> endHandler) {
    return this;
  }
}

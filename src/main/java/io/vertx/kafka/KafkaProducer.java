package io.vertx.kafka;

import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.WriteStream;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class KafkaProducer<K, V> implements WriteStream<ProducerRecord<K, V>> {

  public static final int DEFAULT_MAX_SIZE = 1024 * 1024;

  public static <K, V> KafkaProducer<K, V> create(Vertx vertx, Map<String, Object> config) {
    return new KafkaProducer<>(vertx.getOrCreateContext(), config);
  }

  public static <K, V> KafkaProducer<K, V> create(Vertx vertx, Producer<K, V> producer) {
    return new KafkaProducer<>(vertx.getOrCreateContext(), producer);
  }

  private long maxSize = DEFAULT_MAX_SIZE;
  private long size;
  private final Producer<K, V> producer;
  private Handler<Void> drainHandler;
  private Handler<Throwable> exceptionHandler;
  private final Context context;

  private KafkaProducer(Context context, Producer<K, V> producer) {
    this.producer = producer;
    this.context = context;
  }

  private KafkaProducer(Context context, Map<String, Object> props) {
    this.producer = new org.apache.kafka.clients.producer.KafkaProducer<K, V>(props);
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
  public synchronized KafkaProducer<K, V> write(ProducerRecord<K, V> record) {
    int len = len(record.value());
    size += len;
    try {
      // Non blocking
      producer.send(record, (metadata, err) -> {
        // Callback from IO thread
        synchronized (KafkaProducer.this) {
          size -= len;
          long l = maxSize / 2;
          if (size < l && drainHandler != null) {
            Handler<Void> handler = drainHandler;
            drainHandler = null;
            context.runOnContext(handler);
          }
          if (err != null && exceptionHandler != null) {
            Handler<Throwable> handler = exceptionHandler;
            context.runOnContext(v -> handler.handle(err));
          }
        }
      });
    } catch (Exception e) {
      size -= len;
    }
    return this;
  }

  @Override
  public KafkaProducer<K, V> setWriteQueueMaxSize(int size) {
    maxSize = size;
    return this;
  }

  @Override
  public synchronized boolean writeQueueFull() {
    return size >= maxSize;
  }

  @Override
  public synchronized KafkaProducer<K, V> drainHandler(Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }

  @Override
  public void end() {
  }

  @Override
  public KafkaProducer<K, V> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  public void close() {
    producer.close();
  }

  public void close(long timeout, Handler<Void> completionHandler) {
    context.executeBlocking(f -> {
      producer.close(timeout, TimeUnit.MILLISECONDS);
      f.complete();
    }, ar -> completionHandler.handle(null));
  }
}

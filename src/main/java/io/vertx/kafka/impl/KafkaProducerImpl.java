package io.vertx.kafka.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class KafkaProducerImpl<K, V> implements KafkaProducer<K, V> {

  public static <K, V> void create(Vertx vertx, Properties config, Handler<AsyncResult<KafkaProducer<K, V>>> handler) {
    connect(new KafkaProducerImpl<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.producer.KafkaProducer<>(config)), handler);
  }

  public static <K, V> void create(Vertx vertx, Map<String, Object> config, Handler<AsyncResult<KafkaProducer<K, V>>> handler) {
    connect(new KafkaProducerImpl<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.producer.KafkaProducer<>(config)), handler);
  }

  public static <K, V> void create(Vertx vertx, Producer<K, V> producer, Handler<AsyncResult<KafkaProducer<K, V>>> handler) {
    connect(new KafkaProducerImpl<>(vertx.getOrCreateContext(), producer), handler);
  }

  private static <K, V> void connect(KafkaProducerImpl<K, V> producer, Handler<AsyncResult<KafkaProducer<K, V>>> handler) {
    AtomicBoolean done = new AtomicBoolean();
    Context ctx = producer.context;
    ctx.owner().setTimer(2000, id -> {
      if (done.compareAndSet(false, true)) {
        handler.handle(Future.failedFuture("Kafka connect timeout"));
      }
    });
    ctx.executeBlocking(fut -> {
      // This force to connect to Kafka - which can hang
      producer.producer.partitionsFor("the_topic");
      if (done.compareAndSet(false, true)) {
        fut.complete(producer);
      }
    }, handler);
  }

  private long maxSize = DEFAULT_MAX_SIZE;
  private long size;
  private final Producer<K, V> producer;
  private Handler<Void> drainHandler;
  private Handler<Throwable> exceptionHandler;
  private final Context context;

  private KafkaProducerImpl(Context context, Producer<K, V> producer) {
    this.producer = producer;
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
  public synchronized KafkaProducerImpl<K, V> write(ProducerRecord<K, V> record) {
    int len = len(record.value());
    size += len;
    try {
      // Non blocking
      producer.send(record, (metadata, err) -> {
        // Callback from IO thread
        synchronized (KafkaProducerImpl.this) {
          size -= len;
          long lowWaterMark = maxSize / 2;
          if (size < lowWaterMark && drainHandler != null) {
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
  public KafkaProducerImpl<K, V> setWriteQueueMaxSize(int size) {
    maxSize = size;
    return this;
  }

  @Override
  public synchronized boolean writeQueueFull() {
    return size >= maxSize;
  }

  @Override
  public synchronized KafkaProducerImpl<K, V> drainHandler(Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }

  @Override
  public void end() {
  }

  @Override
  public KafkaProducerImpl<K, V> exceptionHandler(Handler<Throwable> handler) {
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

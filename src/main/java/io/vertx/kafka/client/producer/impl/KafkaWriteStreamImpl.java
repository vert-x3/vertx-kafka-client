package io.vertx.kafka.client.producer.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.KafkaCodecs;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class KafkaWriteStreamImpl<K, V> implements KafkaWriteStream<K, V> {

  public static <K, V> KafkaWriteStreamImpl<K, V> create(Vertx vertx, Properties config) {
    return new KafkaWriteStreamImpl<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.producer.KafkaProducer<>(config));
  }

  public static <K, V> KafkaWriteStreamImpl<K, V> create(Vertx vertx, Properties config, Class<K> keyType, Class<V> valueType) {
    Serializer<K> keySerializer = KafkaCodecs.serializer(keyType);
    Serializer<V> valueSerializer = KafkaCodecs.serializer(valueType);
    return new KafkaWriteStreamImpl<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.producer.KafkaProducer<>(config, keySerializer, valueSerializer));
  }

  public static <K, V> KafkaWriteStreamImpl<K, V> create(Vertx vertx, Map<String, Object> config) {
    return new KafkaWriteStreamImpl<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.producer.KafkaProducer<>(config));
  }

  public static <K, V> KafkaWriteStreamImpl<K, V> create(Vertx vertx, Map<String, Object> config, Class<K> keyType, Class<V> valueType) {
    Serializer<K> keySerializer = KafkaCodecs.serializer(keyType);
    Serializer<V> valueSerializer = KafkaCodecs.serializer(valueType);
    return new KafkaWriteStreamImpl<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.producer.KafkaProducer<>(config, keySerializer, valueSerializer));
  }

  public static <K, V> void create(Vertx vertx, Properties config, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    connect(new KafkaWriteStreamImpl<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.producer.KafkaProducer<>(config)), handler);
  }

  public static <K, V> void create(Vertx vertx, Properties config, Class<K> keyType, Class<V> valueType, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    Serializer<K> keySerializer = KafkaCodecs.serializer(keyType);
    Serializer<V> valueSerializer = KafkaCodecs.serializer(valueType);
    connect(new KafkaWriteStreamImpl<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.producer.KafkaProducer<>(config, keySerializer, valueSerializer)), handler);
  }

  public static <K, V> void create(Vertx vertx, Map<String, Object> config, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    connect(new KafkaWriteStreamImpl<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.producer.KafkaProducer<>(config)), handler);
  }

  public static <K, V> void create(Vertx vertx, Map<String, Object> config, Class<K> keyType, Class<V> valueType, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    Serializer<K> keySerializer = KafkaCodecs.serializer(keyType);
    Serializer<V> valueSerializer = KafkaCodecs.serializer(valueType);
    connect(new KafkaWriteStreamImpl<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.producer.KafkaProducer<>(config, keySerializer, valueSerializer)), handler);
  }

  public static <K, V> void create(Vertx vertx, Producer<K, V> producer, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    connect(new KafkaWriteStreamImpl<>(vertx.getOrCreateContext(), producer), handler);
  }

  private static <K, V> void connect(KafkaWriteStreamImpl<K, V> producer, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    AtomicBoolean done = new AtomicBoolean();
    Context ctx = producer.context;
    ctx.owner().setTimer(2000, id -> {
      if (done.compareAndSet(false, true)) {
        handler.handle(Future.failedFuture("Kafka connect timeout"));
      }
    });
    ctx.executeBlocking(future -> {
      // This force to connect to Kafka - which can hang
      producer.producer.partitionsFor("the_topic");
      if (done.compareAndSet(false, true)) {
        future.complete(producer);
      }
    }, handler);
  }

  private long maxSize = DEFAULT_MAX_SIZE;
  private long size;
  private final Producer<K, V> producer;
  private Handler<Void> drainHandler;
  private Handler<Throwable> exceptionHandler;
  private final Context context;

  private KafkaWriteStreamImpl(Context context, Producer<K, V> producer) {
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
  public synchronized KafkaWriteStreamImpl<K, V> write(ProducerRecord<K, V> record, Handler<RecordMetadata> handler) {

    int len = this.len(record.value());
    this.size += len;

    try {

      // non blocking
      this.producer.send(record, (metadata, err) -> {

        // callback from IO thread
        synchronized (KafkaWriteStreamImpl.this) {

          // if exception happens, no record written
          if (err != null) {

            if (this.exceptionHandler != null) {
              Handler<Throwable> exceptionHandler = this.exceptionHandler;
              this.context.runOnContext(v -> exceptionHandler.handle(err));
            }

          // no error, record written
          } else {

            this.size -= len;

            if (handler != null) {
              handler.handle(metadata);
            }

            long lowWaterMark = this.maxSize / 2;
            if (this.size < lowWaterMark && this.drainHandler != null) {
              Handler<Void> drainHandler = this.drainHandler;
              this.drainHandler = null;
              this.context.runOnContext(drainHandler);
            }
          }

        }
      });

    } catch (Exception e) {
      this.size -= len;
    }

    return this;
  }

  @Override
  public KafkaWriteStreamImpl<K, V> write(ProducerRecord<K, V> record) {

    return this.write(record, null);
  }

  @Override
  public KafkaWriteStreamImpl<K, V> setWriteQueueMaxSize(int size) {
    this.maxSize = size;
    return this;
  }

  @Override
  public synchronized boolean writeQueueFull() {
    return (this.size >= this.maxSize);
  }

  @Override
  public synchronized KafkaWriteStreamImpl<K, V> drainHandler(Handler<Void> handler) {
    this.drainHandler = handler;
    return this;
  }

  @Override
  public void end() {
  }

  @Override
  public KafkaWriteStreamImpl<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.exceptionHandler = handler;
    return this;
  }

  @Override
  public KafkaWriteStreamImpl<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler) {

    AtomicBoolean done = new AtomicBoolean();

    // TODO: should be this timeout related to the Kafka producer property "metadata.fetch.timeout.ms" ?
    this.context.owner().setTimer(2000, id -> {
      if (done.compareAndSet(false, true)) {
        handler.handle(Future.failedFuture("Kafka connect timeout"));
      }
    });

    this.context.executeBlocking(future -> {

      List<PartitionInfo> partitions = this.producer.partitionsFor(topic);
      if (done.compareAndSet(false, true)) {
        future.complete(partitions);
      }
    }, handler);

    return this;
  }

  @Override
  public KafkaWriteStreamImpl<K, V> flush(Handler<Void> completionHandler) {

    this.context.executeBlocking(future -> {

      this.producer.flush();
      future.complete();

    }, ar -> completionHandler.handle(null));

    return this;
  }

  public void close() {
    producer.close();
  }

  public void close(long timeout, Handler<Void> completionHandler) {

    this.context.executeBlocking(future -> {

      this.producer.close(timeout, TimeUnit.MILLISECONDS);
      future.complete();
    }, ar -> completionHandler.handle(null));
  }
}

package io.vertx.kafka.client.producer;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.WriteStream;
import io.vertx.kafka.client.common.KafkaPartitionInfo;
import io.vertx.kafka.client.producer.impl.KafkaProducerImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author <a href="mailto:ppatierno@live.com">Paolo Patierno</a>
 */
@VertxGen
public interface KafkaProducer<K, V> extends WriteStream<KafkaProducerRecord<K, V>> {

  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Map<String, String> config) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, new HashMap<>(config));
    return new KafkaProducerImpl<>(stream);
  }

  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Map<String, String> config, Class<K> keyType, Class<V> valueType) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, new HashMap<>(config), keyType, valueType);
    return new KafkaProducerImpl<>(stream);
  }

  @GenIgnore
  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Properties config) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, config);
    return new KafkaProducerImpl<>(stream);
  }

  @GenIgnore
  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Properties config, Class<K> keyType, Class<V> valueType) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, config, keyType, valueType);
    return new KafkaProducerImpl<>(stream);
  }

  @Fluent
  @Override
  KafkaProducer<K, V> exceptionHandler(Handler<Throwable> handler);

  @Fluent
  @Override
  KafkaProducer<K, V> write(KafkaProducerRecord<K, V> kafkaProducerRecord);

  @Override
  void end();

  @Override
  void end(KafkaProducerRecord<K, V> kafkaProducerRecord);

  @Fluent
  @Override
  KafkaProducer<K, V> setWriteQueueMaxSize(int i);

  @Override
  boolean writeQueueFull();

  @Fluent
  @Override
  KafkaProducer<K, V> drainHandler(Handler<Void> handler);

  @Fluent
  KafkaProducer<K, V> write(KafkaProducerRecord<K, V> kafkaProducerRecord, Handler<KafkaRecordMetadata> handler);

  @Fluent
  KafkaProducer<K, V> partitionsFor(String topic, Handler<AsyncResult<List<KafkaPartitionInfo>>> handler);

  @Fluent
  KafkaProducer<K, V> flush(Handler<Void> completionHandler);

  void close();

  void close(long timeout, Handler<Void> completionHandler);

  @GenIgnore
  KafkaWriteStream<K, V> asStream();
}

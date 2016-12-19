package io.vertx.kafka.client.producer;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.WriteStream;
import io.vertx.kafka.client.producer.impl.KafkaProducerImpl;

import java.util.Map;
import java.util.Properties;

/**
 * @author <a href="mailto:ppatierno@live.com">Paolo Patierno</a>
 */
@VertxGen
public interface KafkaProducer<K, V> extends WriteStream<KafkaProducerRecord<K, V>> {

  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Properties config) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, config);
    return new KafkaProducerImpl<>(stream);
  }

  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Properties config, Class<K> keyType, Class<V> valueType) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, config, keyType, valueType);
    return new KafkaProducerImpl<>(stream);
  }

  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Map<String, Object> config) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, config);
    return new KafkaProducerImpl<>(stream);
  }

  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Map<String, Object> config, Class<K> keyType, Class<V> valueType) {
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
}

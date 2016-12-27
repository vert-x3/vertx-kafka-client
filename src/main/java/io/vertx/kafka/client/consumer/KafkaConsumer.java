package io.vertx.kafka.client.consumer;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.ReadStream;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@VertxGen
public interface KafkaConsumer<K, V> extends ReadStream<KafkaConsumerRecord<K, V>> {

  static <K, V> KafkaConsumer<K, V> create(Vertx vertx, ConsumerOptions options, Map<String, String> config) {
    KafkaReadStream<K, V> stream = KafkaReadStream.create(vertx, options, new HashMap<>(config));
    return new KafkaConsumerImpl<>(stream);
  }

  static <K, V> KafkaConsumer<K, V> create(Vertx vertx, ConsumerOptions options, Map<String, String> config,
                                           Class<K> keyType, Class<V> valueType) {
    KafkaReadStream<K, V> stream = KafkaReadStream.create(vertx, options, new HashMap<>(config), keyType, valueType);
    return new KafkaConsumerImpl<>(stream);
  }

  @GenIgnore
  static <K, V> KafkaConsumer<K, V> create(Vertx vertx, ConsumerOptions options, Properties config) {
    KafkaReadStream<K, V> stream = KafkaReadStream.create(vertx, options, config);
    return new KafkaConsumerImpl<>(stream);
  }

  @GenIgnore
  static <K, V> KafkaConsumer<K, V> create(Vertx vertx, ConsumerOptions options, Properties config,
                                           Class<K> keyType, Class<V> valueType) {
    KafkaReadStream<K, V> stream = KafkaReadStream.create(vertx, options, config, keyType, valueType);
    return new KafkaConsumerImpl<>(stream);
  }

  @Fluent
  @Override
  KafkaConsumer<K, V> exceptionHandler(Handler<Throwable> handler);

  @Fluent
  @Override
  KafkaConsumer<K, V> handler(Handler<KafkaConsumerRecord<K, V>> handler);

  @Fluent
  @Override
  KafkaConsumer<K, V> pause();

  @Fluent
  @Override
  KafkaConsumer<K, V> resume();

  @Fluent
  @Override
  KafkaConsumer<K, V> endHandler(Handler<Void> endHandler);

  @Fluent
  KafkaConsumer<K, V> subscribe(Set<String> topics);

  @Fluent
  KafkaConsumer<K, V> subscribe(Set<String> topics, Handler<AsyncResult<Void>> completionHandler);

  @Fluent
  KafkaConsumer<K, V> unsubscribe();

  @Fluent
  KafkaConsumer<K, V> unsubscribe(Handler<AsyncResult<Void>> completionHandler);

  @Fluent
  KafkaConsumer<K, V> pause(Set<TopicPartition> topicPartitions);

  @Fluent
  KafkaConsumer<K, V> pause(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler);

  @Fluent
  KafkaConsumer<K, V> resume(Set<TopicPartition> topicPartitions);

  @Fluent
  KafkaConsumer<K, V> resume(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler);

  @Fluent
  KafkaConsumer<K, V> partitionsRevokedHandler(Handler<Set<TopicPartition>> handler);

  @Fluent
  KafkaConsumer<K, V> partitionsAssignedHandler(Handler<Set<TopicPartition>> handler);

  void commit();

  void commit(Handler<AsyncResult<Void>> completionHandler);

  default void close() {
    close(null);
  }

  void close(Handler<Void> completionHandler);

  @GenIgnore
  KafkaReadStream<K, V> asStream();

}

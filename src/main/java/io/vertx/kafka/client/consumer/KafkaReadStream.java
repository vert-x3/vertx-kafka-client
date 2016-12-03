package io.vertx.kafka.client.consumer;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.ReadStream;
import io.vertx.kafka.client.KafkaCodecs;
import io.vertx.kafka.client.consumer.impl.EventLoopThreadConsumer;
import io.vertx.kafka.client.consumer.impl.WorkerThreadConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A {@link ReadStream} for consuming Kafka {@link ConsumerRecord}.
 * <p>
 * The {@code pause()} and {@code resume()} provides global control over reading the records from the consumer.
 * <p>
 * The {@link #pause(Collection)} and {@link #resume(Collection)} provides finer grained control over reading records
 * for specific Topic/Partition, these are Kafka's specific operations.
 *
 * note : should we provide ReadStream for specific Topic/Partition ?
 *
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public interface KafkaReadStream<K, V> extends ReadStream<ConsumerRecord<K, V>> {

  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, ConsumerOptions options, Properties config) {
    return create(vertx, options,  new org.apache.kafka.clients.consumer.KafkaConsumer<>(config));
  }

  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, ConsumerOptions options, Properties config, Class<K> keyType, Class<V> valueType) {
    Deserializer<K> keyDeserializer = KafkaCodecs.deserializer(keyType);
    Deserializer<V> valueDeserializer = KafkaCodecs.deserializer(valueType);
    return create(vertx, options,  new org.apache.kafka.clients.consumer.KafkaConsumer<>(config, keyDeserializer, valueDeserializer));
  }

  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, ConsumerOptions options, Map<String, Object> config) {
    return create(vertx, options, new org.apache.kafka.clients.consumer.KafkaConsumer<>(config));
  }

  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, ConsumerOptions options, Map<String, Object> config, Class<K> keyType, Class<V> valueType) {
    Deserializer<K> keyDeserializer = KafkaCodecs.deserializer(keyType);
    Deserializer<V> valueDeserializer = KafkaCodecs.deserializer(valueType);
    return create(vertx, options, new org.apache.kafka.clients.consumer.KafkaConsumer<>(config, keyDeserializer, valueDeserializer));
  }

  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, ConsumerOptions options, Consumer<K, V> consumer) {
    if (options.isWorkerThread()) {
      return new WorkerThreadConsumer<>(vertx.getOrCreateContext(), consumer);
    } else {
      return new EventLoopThreadConsumer<>(vertx.getOrCreateContext(), consumer);
    }
  }

  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, Properties config) {
    return create(vertx, new ConsumerOptions(), config);
  }

  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, Map<String, Object> config) {
    return create(vertx, new ConsumerOptions(), config);
  }

  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, Consumer<K, V> consumer) {
    return create(vertx, new ConsumerOptions(), consumer);
  }

  void commited(TopicPartition topicPartition, Handler<AsyncResult<OffsetAndMetadata>> handler);

  KafkaReadStream<K, V> pause(Collection<TopicPartition> topicPartitions);

  KafkaReadStream<K, V> pause(Collection<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler);

  KafkaReadStream<K, V> resume(Collection<TopicPartition> topicPartitions);

  KafkaReadStream<K, V> resume(Collection<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler);

  KafkaReadStream<K, V> seekToEnd(Collection<TopicPartition> topicPartitions);

  KafkaReadStream<K, V> seekToEnd(Collection<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler);

  KafkaReadStream<K, V> seekToBeginning(Collection<TopicPartition> topicPartitions);

  KafkaReadStream<K, V> seekToBeginning(Collection<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler);

  KafkaReadStream<K, V> seek(TopicPartition topicPartition, long offset);

  KafkaReadStream<K, V> seek(TopicPartition topicPartition, long offset, Handler<AsyncResult<Void>> completionHandler);

  KafkaReadStream<K, V> partitionsRevokedHandler(Handler<Collection<TopicPartition>> handler);

  KafkaReadStream<K, V> partitionsAssignedHandler(Handler<Collection<TopicPartition>> handler);

  KafkaReadStream<K, V> subscribe(Set<String> topics);

  KafkaReadStream<K, V> subscribe(Set<String> topics, Handler<AsyncResult<Void>> handler);

  void commit();

  void commit(Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler);

  void commit(Map<TopicPartition, OffsetAndMetadata> offsets);

  void commit(Map<TopicPartition, OffsetAndMetadata> offsets, Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler);

  default void close() {
    close(null);
  }

  void close(Handler<Void> completionHandler);

}

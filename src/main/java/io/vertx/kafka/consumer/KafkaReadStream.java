package io.vertx.kafka.consumer;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.ReadStream;
import io.vertx.kafka.consumer.impl.WorkerThreadConsumer;
import io.vertx.kafka.consumer.impl.EventLoopThreadConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public interface KafkaReadStream<K, V> extends ReadStream<ConsumerRecord<K, V>> {

  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, ConsumerOptions options, Properties config) {
    return create(vertx, options,  new org.apache.kafka.clients.consumer.KafkaConsumer<>(config));
  }

  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, ConsumerOptions options, Map<String, Object> config) {
    return create(vertx, options, new org.apache.kafka.clients.consumer.KafkaConsumer<>(config));
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

package io.vertx.kafka;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.ReadStream;
import io.vertx.kafka.impl.WorkerThreadConsumer;
import io.vertx.kafka.impl.EventLoopThreadConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public interface KafkaConsumer<K, V> extends ReadStream<ConsumerRecord<K, V>> {

  static <K, V> KafkaConsumer<K, V> create(Vertx vertx, ConsumerOptions options, Properties config) {
    return create(vertx, options,  new org.apache.kafka.clients.consumer.KafkaConsumer<>(config));
  }

  static <K, V> KafkaConsumer<K, V> create(Vertx vertx, ConsumerOptions options, Map<String, Object> config) {
    return create(vertx, options, new org.apache.kafka.clients.consumer.KafkaConsumer<>(config));
  }

  static <K, V> KafkaConsumer<K, V> create(Vertx vertx, ConsumerOptions options, Consumer<K, V> consumer) {
    if (options.isWorkerThread()) {
      return new WorkerThreadConsumer<>(vertx.getOrCreateContext(), consumer);
    } else {
      return new EventLoopThreadConsumer<>(vertx.getOrCreateContext(), consumer);
    }
  }

  static <K, V> KafkaConsumer<K, V> create(Vertx vertx, Properties config) {
    return create(vertx, new ConsumerOptions(), config);
  }

  static <K, V> KafkaConsumer<K, V> create(Vertx vertx, Map<String, Object> config) {
    return create(vertx, new ConsumerOptions(), config);
  }

  static <K, V> KafkaConsumer<K, V> create(Vertx vertx, Consumer<K, V> consumer) {
    return create(vertx, new ConsumerOptions(), consumer);
  }

  KafkaConsumer<K, V> subscribe(Set<String> topics);

  KafkaConsumer<K, V> subscribe(Set<String> topics, Handler<Void> handler);

  default void close() {
    close(null);
  }

  void close(Handler<Void> completionHandler);

}

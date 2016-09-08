package io.vertx.kafka;

import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Set;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public interface KafkaConsumer<K, V> extends ReadStream<ConsumerRecord<K, V>> {

  KafkaConsumer<K, V> subscribe(Set<String> topics);

  default void close() {
    close(null);
  }

  void close(Handler<Void> completionHandler);

}

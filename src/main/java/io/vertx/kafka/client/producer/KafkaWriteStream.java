package io.vertx.kafka.client.producer;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.WriteStream;
import io.vertx.kafka.client.producer.impl.KafkaWriteStreamImpl;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public interface KafkaWriteStream<K, V> extends WriteStream<ProducerRecord<K, V>> {

  int DEFAULT_MAX_SIZE = 1024 * 1024;

  static <K, V> void create(Vertx vertx, Properties config, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    KafkaWriteStreamImpl.create(vertx, config, handler);
  }

  static <K, V> void create(Vertx vertx, Map<String, Object> config, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    KafkaWriteStreamImpl.create(vertx, config, handler);
  }

  static <K, V> void create(Vertx vertx, Producer<K, V> producer, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    KafkaWriteStreamImpl.create(vertx, producer, handler);
  }

  void close();

  void close(long timeout, Handler<Void> completionHandler);
}

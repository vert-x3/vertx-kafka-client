package io.vertx.kafka.client.producer;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.WriteStream;
import io.vertx.kafka.client.producer.impl.KafkaWriteStreamImpl;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public interface KafkaWriteStream<K, V> extends WriteStream<ProducerRecord<K, V>> {

  int DEFAULT_MAX_SIZE = 1024 * 1024;

  static <K, V> KafkaWriteStream<K, V> create(Vertx vertx, Properties config) {
    return KafkaWriteStreamImpl.create(vertx, config);
  }

  static <K, V> KafkaWriteStream<K, V> create(Vertx vertx, Properties config, Class<K> keyType, Class<V> valueType) {
    return KafkaWriteStreamImpl.create(vertx, config, keyType, valueType);
  }

  static <K, V> KafkaWriteStream<K, V> create(Vertx vertx, Map<String, Object> config) {
    return KafkaWriteStreamImpl.create(vertx, config);
  }

  static <K, V> KafkaWriteStream<K, V> create(Vertx vertx, Map<String, Object> config, Class<K> keyType, Class<V> valueType) {
    return KafkaWriteStreamImpl.create(vertx, config, keyType, valueType);
  }

  static <K, V> void create(Vertx vertx, Properties config, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    KafkaWriteStreamImpl.create(vertx, config, handler);
  }

  static <K, V> void create(Vertx vertx, Properties config, Class<K> keyType, Class<V> valueType, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    KafkaWriteStreamImpl.create(vertx, config, keyType, valueType, handler);
  }

  static <K, V> void create(Vertx vertx, Map<String, Object> config, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    KafkaWriteStreamImpl.create(vertx, config, handler);
  }

  static <K, V> void create(Vertx vertx, Map<String, Object> config, Class<K> keyType, Class<V> valueType, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    KafkaWriteStreamImpl.create(vertx, config, keyType, valueType, handler);
  }

  static <K, V> void create(Vertx vertx, Producer<K, V> producer, Handler<AsyncResult<KafkaWriteStream<K, V>>> handler) {
    KafkaWriteStreamImpl.create(vertx, producer, handler);
  }

  KafkaWriteStream<K, V> write(ProducerRecord<K, V> record, Handler<RecordMetadata> handler);

  KafkaWriteStream<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler);

  KafkaWriteStream<K, V> flush(Handler<Void> completionHandler);

  void close();

  void close(long timeout, Handler<Void> completionHandler);
}

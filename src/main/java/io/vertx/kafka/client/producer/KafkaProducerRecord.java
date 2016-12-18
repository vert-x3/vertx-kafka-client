package io.vertx.kafka.client.producer;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author <a href="mailto:ppatierno@live.com">Paolo Patierno</a>
 */
@VertxGen
public interface KafkaProducerRecord<K, V> {

  /**
   * The topic this record is being sent to
   *
   * @return
   */
  String topic();

  /**
   * The key (or null if no key is specified)
   *
   * @return
   */
  K key();

  /**
   * The value
   *
   * @return
   */
  V value();

  /**
   * The timestamp of this record
   *
   * @return
   */
  long timestamp();

  /**
   * The partition to which the record will be sent (or null if no partition was specified)
   *
   * @return
   */
  int partition();

  /**
   * Kafka producer record with backed information
   *
   * @return
   */
  @GenIgnore
  ProducerRecord record();
}

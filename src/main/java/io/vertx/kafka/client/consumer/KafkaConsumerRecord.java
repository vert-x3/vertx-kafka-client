package io.vertx.kafka.client.consumer;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@VertxGen
public interface KafkaConsumerRecord<K, V> {

  /**
   * The topic this record is received from
   *
   * @return
   */
  String topic();

  /**
   * The partition from which this record is received
   *
   * @return
   */
  int partition();

  /**
   * The position of this record in the corresponding Kafka partition.
   *
   * @return
   */
  long offset();

  /**
   * The timestamp of this record
   *
   * @return
   */
  long timestamp();

  /**
   * The timestamp type of this record
   *
   * @return
   */
  TimestampType timestampType();

  /**
   * The checksum (CRC32) of the record.
   *
   * @return
   */
  long checksum();

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
   * Kafka consumer record with backed information
   *
   * @return
   */
  @GenIgnore
  ConsumerRecord record();
}

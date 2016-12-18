package io.vertx.kafka.client.consumer.impl;

import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class KafkaConsumerRecordImpl<K, V> implements KafkaConsumerRecord<K, V> {

  private final ConsumerRecord<K, V> record;

  /**
   * Constructor
   *
   * @param record  Kafka consumer record for backing information
   */
  public KafkaConsumerRecordImpl(ConsumerRecord<K, V> record) {
    this.record = record;
  }

  @Override
  public String topic() {
    return this.record.topic();
  }

  @Override
  public int partition() {
    return this.record.partition();
  }

  @Override
  public long offset() {
    return this.record.offset();
  }

  @Override
  public long timestamp() {
    return this.record.timestamp();
  }

  @Override
  public TimestampType timestampType() {
    return this.record.timestampType();
  }

  @Override
  public long checksum() {
    return this.record.checksum();
  }

  @Override
  public K key() {
    return this.record.key();
  }

  @Override
  public V value() {
    return this.record.value();
  }

  @Override
  public ConsumerRecord record() {
    return this.record;
  }
}

package io.vertx.kafka.client.consumer.impl;

import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class KafkaConsumerRecordImpl<K, V> implements KafkaConsumerRecord<K, V> {

  private final ConsumerRecord<K, V> record;

  public KafkaConsumerRecordImpl(ConsumerRecord<K, V> record) {
    this.record = record;
  }

  @Override
  public String topic() {
    return record.topic();
  }

  @Override
  public int partition() {
    return record.partition();
  }

  @Override
  public long offset() {
    return record.offset();
  }

  @Override
  public long timestamp() {
    return record.timestamp();
  }

  @Override
  public TimestampType timestampType() {
    return record.timestampType();
  }

  @Override
  public long checksum() {
    return record.checksum();
  }

  @Override
  public K key() {
    return record.key();
  }

  @Override
  public V value() {
    return record.value();
  }

  @Override
  public ConsumerRecord record() {
    return record;
  }
}

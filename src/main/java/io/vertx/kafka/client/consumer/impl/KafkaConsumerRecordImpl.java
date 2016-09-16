package io.vertx.kafka.client.consumer.impl;

import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class KafkaConsumerRecordImpl implements KafkaConsumerRecord {

  private final ConsumerRecord<?, ?> record;

  public KafkaConsumerRecordImpl(ConsumerRecord<?, ?> record) {
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
  public <K> K key() {
    return (K) record.key();
  }

  @Override
  public <V> V value() {
    return (V) record.value();
  }
}

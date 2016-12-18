package io.vertx.kafka.client.producer.impl;

import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author <a href="mailto:ppatierno@live.com">Paolo Patierno</a>
 */
public class KafkaProducerRecordImpl<K, V> implements KafkaProducerRecord<K, V> {

  private final ProducerRecord<K, V> record;

  /**
   * Constructor
   *
   * @param record  Kafka producer record for backing information
   */
  public KafkaProducerRecordImpl(ProducerRecord<K, V> record) {
    this.record = record;
  }

  @Override
  public String topic() {
    return this.record.topic();
  }

  @Override
  public K key() {
    return this.record.key();
  }

  @Override
  public long timestamp() {
    return this.record.timestamp();
  }

  @Override
  public V value() {
    return this.record.value();
  }

  @Override
  public int partition() {
    return this.record.partition();
  }

  @Override
  public ProducerRecord record() {
    return this.record;
  }
}

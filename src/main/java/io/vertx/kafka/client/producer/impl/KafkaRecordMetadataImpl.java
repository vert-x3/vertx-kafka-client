package io.vertx.kafka.client.producer.impl;

import io.vertx.kafka.client.producer.KafkaRecordMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author <a href="mailto:ppatierno@live.com">Paolo Patierno</a>
 */
public class KafkaRecordMetadataImpl implements KafkaRecordMetadata {

  private final RecordMetadata record;

  /**
   * Constructor
   *
   * @param record  Kafka record metadata for backing information
   */
  public KafkaRecordMetadataImpl(RecordMetadata record) {
    this.record = record;
  }

  @Override
  public long checksum() {
    return this.record.checksum();
  }

  @Override
  public long offset() {
    return this.record.offset();
  }

  @Override
  public int partition() {
    return this.record.partition();
  }

  @Override
  public long timestamp() {
    return this.record.timestamp();
  }

  @Override
  public String topic() {
    return this.record.topic();
  }

  @Override
  public RecordMetadata record() {
    return this.record;
  }
}

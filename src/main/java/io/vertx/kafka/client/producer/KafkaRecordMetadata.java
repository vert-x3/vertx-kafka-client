package io.vertx.kafka.client.producer;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author <a href="mailto:ppatierno@live.com">Paolo Patierno</a>
 */
@VertxGen
public interface KafkaRecordMetadata {

  /**
   * The checksum (CRC32) of the record.
   *
   * @return
   */
  long checksum();

  /**
   * The offset of the record in the topic/partition.
   *
   * @return
   */
  long offset();

  /**
   * The partition the record was sent to
   *
   * @return
   */
  int partition();

  /**
   * The timestamp of the record in the topic/partition
   *
   * @return
   */
  long timestamp();

  /**
   * The topic the record was appended to
   *
   * @return
   */
  String topic();

  /**
   * Kafka record metadata with backed information
   *
   * @return
   */
  @GenIgnore
  RecordMetadata record();
}

package io.vertx.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;

/**
 * Vert.x Kafka consumer records
 */
@VertxGen
public interface KafkaConsumerRecords<K, V> {
  /**
   * @return the total number of records in this batch
   */
  int size();
  /**
   * @return whether this batch contains any records
   */
  boolean isEmpty();
  /**
   * Get the record at the given index
   * @param index the index of the record to get
   * @throws IndexOutOfBoundsException if index <0 or index>={@link #size()}
   */
  KafkaConsumerRecord<K, V> recordAt(int index);
  
  /**
   * @return  the native Kafka consumer records with backed information
   */
  @GenIgnore
  ConsumerRecords<K, V> records();
  
}

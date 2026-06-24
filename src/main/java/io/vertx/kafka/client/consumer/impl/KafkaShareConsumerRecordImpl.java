package io.vertx.kafka.client.consumer.impl;

import io.vertx.kafka.client.consumer.KafkaShareConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaShareConsumerRecordImpl<K, V> extends KafkaConsumerRecordImpl<K, V> implements KafkaShareConsumerRecord<K, V> {

  public KafkaShareConsumerRecordImpl(ConsumerRecord<K, V> record) {
    super(record);
  }

  @Override
  public int deliveryCount() {
    return record().deliveryCount()
      .map(Short::intValue)
      .orElse(0);
  }

  @Override
  public String toString() {
    return "KafkaShareConsumerRecord{" +
      "topic=" + topic() +
      ",partition=" + partition() +
      ",offset=" + offset() +
      ",timestamp=" + timestamp() +
      ",key=" + key() +
      ",value=" + value() +
      ",headers=" + headers() +
      ",deliveryCount=" + deliveryCount() +
      "}";
  }
}

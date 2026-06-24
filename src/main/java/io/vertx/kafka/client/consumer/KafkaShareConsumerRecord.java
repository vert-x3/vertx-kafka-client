package io.vertx.kafka.client.consumer;

import io.vertx.codegen.annotations.VertxGen;

/**
 * Vert.x Kafka share consumer record
 */
@VertxGen
public interface KafkaShareConsumerRecord<K, V> extends KafkaConsumerRecord<K, V> {
  int deliveryCount();
}

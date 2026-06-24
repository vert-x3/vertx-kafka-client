package io.vertx.kafka.client.consumer;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import org.apache.kafka.clients.consumer.ConsumerRecords;

@VertxGen
public interface KafkaShareConsumerRecords<K, V> {

  int size();

  boolean isEmpty();

  KafkaShareConsumerRecord<K, V> recordAt(int index);

  @GenIgnore
  ConsumerRecords<K, V> records();
}

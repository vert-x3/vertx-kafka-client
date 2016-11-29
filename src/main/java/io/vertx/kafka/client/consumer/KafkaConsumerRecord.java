package io.vertx.kafka.client.consumer;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@VertxGen
public interface KafkaConsumerRecord<K, V> {

  String topic();
  int partition();
  long offset();
  long timestamp();
  TimestampType timestampType();
  long checksum();
  K key();
  V value();

  @GenIgnore
  ConsumerRecord record();
  
}

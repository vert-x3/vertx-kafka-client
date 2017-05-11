package io.vertx.kafka.client.consumer.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;

public class KafkaConsumerRecordsImpl<K, V> implements KafkaConsumerRecords<K, V>{

  private final ConsumerRecords<K, V> records;
  private List<KafkaConsumerRecord<K, V>> list;

  public KafkaConsumerRecordsImpl(ConsumerRecords<K, V> records) {
    this.records = records;
  }
  
  @Override
  public int size() {
    return records.count();
  }

  @Override
  public boolean isEmpty() {
    return records.isEmpty();
  }

  @Override
  public KafkaConsumerRecord<K, V> recordAt(int index) {
    if (list == null) {
      list = new ArrayList<>(records.count());
      records.forEach(r -> list.add(new KafkaConsumerRecordImpl<K, V>(r)));
    }
    return list.get(index);
  }

  @Override
  public ConsumerRecords<K, V> records() {
    return records;
  }

}

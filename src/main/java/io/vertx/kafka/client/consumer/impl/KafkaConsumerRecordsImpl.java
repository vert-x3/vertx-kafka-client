/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
      records.forEach(record -> list.add(new KafkaConsumerRecordImpl<K, V>(record)));
    }
    return list.get(index);
  }

  @Override
  public ConsumerRecords<K, V> records() {
    return records;
  }

}

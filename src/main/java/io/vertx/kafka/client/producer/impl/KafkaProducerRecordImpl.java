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

package io.vertx.kafka.client.producer.impl;

import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Vert.x Kafka producer record implementation
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

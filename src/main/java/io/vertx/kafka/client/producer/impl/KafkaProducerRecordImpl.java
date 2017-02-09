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
   * @param topic the topic this record is being sent to
   * @param key the key (or null if no key is specified)
   * @param value the value
   * @param timestamp the timestamp of this record
   * @param partition the partition to which the record will be sent (or null if no partition was specified)
   */
  public KafkaProducerRecordImpl(String topic, K key, V value, Long timestamp, Integer partition) {

    this.record = new ProducerRecord<>(topic, partition, timestamp, key, value);
  }

  /**
   * Constructor
   *
   * @param topic the topic this record is being sent to
   * @param key the key (or null if no key is specified)
   * @param value the value
   * @param partition the partition to which the record will be sent (or null if no partition was specified)
   */
  public KafkaProducerRecordImpl(String topic, K key, V value, Integer partition) {

    this.record = new ProducerRecord<>(topic, partition, key, value);
  }

  /**
   * Constructor
   *
   * @param topic the topic this record is being sent to
   * @param key the key (or null if no key is specified)
   * @param value the value
   */
  public KafkaProducerRecordImpl(String topic, K key, V value) {

    this.record = new ProducerRecord<>(topic, key, value);
  }

  /**
   * Constructor
   *
   * @param topic the topic this record is being sent to
   * @param value the value
   */
  public KafkaProducerRecordImpl(String topic, V value) {

    this.record = new ProducerRecord<>(topic, value);
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
  public Long timestamp() {
    return this.record.timestamp();
  }

  @Override
  public V value() {
    return this.record.value();
  }

  @Override
  public Integer partition() {
    return this.record.partition();
  }

  @Override
  public ProducerRecord record() {
    return this.record;
  }
}

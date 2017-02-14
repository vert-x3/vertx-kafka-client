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

import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

/**
 * Vert.x Kafka consumer record implementation
 */
public class KafkaConsumerRecordImpl<K, V> implements KafkaConsumerRecord<K, V> {

  private final ConsumerRecord<K, V> record;

  /**
   * Constructor
   *
   * @param record  Kafka consumer record for backing information
   */
  public KafkaConsumerRecordImpl(ConsumerRecord<K, V> record) {
    this.record = record;
  }

  @Override
  public String topic() {
    return this.record.topic();
  }

  @Override
  public int partition() {
    return this.record.partition();
  }

  @Override
  public long offset() {
    return this.record.offset();
  }

  @Override
  public long timestamp() {
    return this.record.timestamp();
  }

  @Override
  public TimestampType timestampType() {
    return this.record.timestampType();
  }

  @Override
  public long checksum() {
    return this.record.checksum();
  }

  @Override
  public K key() {
    return this.record.key();
  }

  @Override
  public V value() {
    return this.record.value();
  }

  @Override
  public ConsumerRecord record() {
    return this.record;
  }
}

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

package io.vertx.kafka.client.producer;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Vert.x Kafka producer record
 */
@VertxGen
public interface KafkaProducerRecord<K, V> {

  /**
   * @return  the topic this record is being sent to
   */
  String topic();

  /**
   * @return  the key (or null if no key is specified)
   */
  K key();

  /**
   * @return  the value
   */
  V value();

  /**
   * @return  the timestamp of this record
   */
  long timestamp();

  /**
   * @return  the partition to which the record will be sent (or null if no partition was specified)
   */
  int partition();

  /**
   * @return  the native Kafka producer record with backed information
   */
  @GenIgnore
  ProducerRecord record();
}

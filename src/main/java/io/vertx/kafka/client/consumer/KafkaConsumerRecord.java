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

package io.vertx.kafka.client.consumer;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

import java.util.List;

/**
 * Vert.x Kafka consumer record
 */
@VertxGen
public interface KafkaConsumerRecord<K, V> {

  /**
   * @return  the topic this record is received from
   */
  String topic();

  /**
   * @return  the partition from which this record is received
   */
  int partition();

  /**
   * @return  the position of this record in the corresponding Kafka partition.
   */
  long offset();

  /**
   * @return  the timestamp of this record
   */
  long timestamp();

  /**
   * @return  the timestamp type of this record
   */
  TimestampType timestampType();

  /**
   * @return  the checksum (CRC32) of the record.
   *
   * @deprecated As of Kafka 0.11.0. Because of the potential for message format conversion on the broker, the
   *             checksum returned by the broker may not match what was computed by the producer.
   *             It is therefore unsafe to depend on this checksum for end-to-end delivery guarantees. Additionally,
   *             message format v2 does not include a record-level checksum (for performance, the record checksum
   *             was replaced with a batch checksum). To maintain compatibility, a partial checksum computed from
   *             the record timestamp, serialized key size, and serialized value size is returned instead, but
   *             this should not be depended on for end-to-end reliability.
   */
  @Deprecated
  long checksum();

  /**
   * @return  the key (or null if no key is specified)
   */
  K key();

  /**
   * @return  the value
   */
  V value();

  /**
   * @return the list of consumer record headers
   */
  List<KafkaHeader> headers();

  /**
   * @return  the native Kafka consumer record with backed information
   */
  @GenIgnore
  ConsumerRecord<K, V> record();
}

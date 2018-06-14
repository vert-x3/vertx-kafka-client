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

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

/**
 * Metadata related to a Kafka record
 */
@DataObject
public class RecordMetadata {

  private long checksum;
  private long offset;
  private int partition;
  private long timestamp;
  private String topic;

  /**
   * Constructor
   */
  public RecordMetadata() {

  }

  /**
   * Constructor
   *
   * @param checksum  checksum (CRC32) of the record
   * @param offset  the offset of the record in the topic/partition.
   * @param partition the partition the record was sent to
   * @param timestamp the timestamp of the record in the topic/partition
   * @param topic the topic the record was appended to
   */
  public RecordMetadata(long checksum, long offset, int partition, long timestamp, String topic) {
    this.checksum = checksum;
    this.offset = offset;
    this.partition = partition;
    this.timestamp = timestamp;
    this.topic = topic;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public RecordMetadata(JsonObject json) {
    this.checksum = json.getLong("checksum");
    this.offset = json.getLong("offset");
    this.partition = json.getInteger("partition");
    this.timestamp = json.getLong("timestamp");
    this.topic = json.getString("topic");
  }

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
  public long checksum() {
    return this.checksum;
  }

  /**
   * Set the checksum (CRC32) of the record.
   *
   * @param checksum  checksum (CRC32) of the record
   * @return  current instance of the class to be fluent
     */
  public RecordMetadata setChecksum(long checksum) {
    this.checksum = checksum;
    return this;
  }

  /**
   * @return  the offset of the record in the topic/partition.
   */
  public long getOffset() {
    return this.offset;
  }

  /**
   * Set the offset of the record in the topic/partition.
   *
   * @param offset  offset of the record in the topic/partition
   * @return  current instance of the class to be fluent
     */
  public RecordMetadata setOffset(long offset) {
    this.offset = offset;
    return this;
  }

  /**
   * @return  the partition the record was sent to
   */
  public int getPartition() {
    return this.partition;
  }

  /**
   * Set the partition the record was sent to
   *
   * @param partition the partition the record was sent to
   * @return  current instance of the class to be fluent
   */
  public RecordMetadata setPartition(int partition) {
    this.partition = partition;
    return this;
  }

  /**
   * @return  the timestamp of the record in the topic/partition
   */
  public long getTimestamp() {
    return this.timestamp;
  }

  /**
   * Set the timestamp of the record in the topic/partition
   *
   * @param timestamp the timestamp of the record in the topic/partition
   * @return  current instance of the class to be fluent
   */
  public RecordMetadata setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  /**
   * @return  the topic the record was appended to
   */
  public String getTopic() {
    return this.topic;
  }

  /**
   * Set the topic the record was appended to
   *
   * @param topic the topic the record was appended to
   * @return  current instance of the class to be fluent
   */
  public RecordMetadata setTopic(String topic) {
    this.topic = topic;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {
    JsonObject jsonObject = new JsonObject();

    jsonObject
      .put("checksum", this.checksum)
      .put("offset", this.offset)
      .put("partition", this.partition)
      .put("timestamp", this.timestamp)
      .put("topic", this.topic);

    return jsonObject;
  }
}

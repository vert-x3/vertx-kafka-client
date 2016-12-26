package io.vertx.kafka.client.producer;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

/**
 * @author <a href="mailto:ppatierno@live.com">Paolo Patierno</a>
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
   * The checksum (CRC32) of the record.
   *
   * @return
   */
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
   * The offset of the record in the topic/partition.
   *
   * @return
   */
  public long offset() {
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
   * The partition the record was sent to
   *
   * @return
   */
  public int partition() {
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
   * The timestamp of the record in the topic/partition
   *
   * @return
   */
  public long timestamp() {
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
   * The topic the record was appended to
   *
   * @return
   */
  public String topic() {
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

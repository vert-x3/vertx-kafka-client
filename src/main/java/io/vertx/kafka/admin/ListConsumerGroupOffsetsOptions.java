package io.vertx.kafka.admin;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.common.TopicPartition;
import java.util.List;

@DataObject(generateConverter = true)
public class ListConsumerGroupOffsetsOptions {

  private List<TopicPartition> topicPartitions = null;

  /**
   * Constructor
   */
  public ListConsumerGroupOffsetsOptions() {}

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public ListConsumerGroupOffsetsOptions(JsonObject json) {
    ListConsumerGroupOffsetsOptionsConverter.fromJson(json, this);
  }

  /**
   * Set the topic partitions to list as part of the result.
   * {@code null} includes all topic partitions.
   *
   * @param topicPartitions List of topic partitions to include
   * @return This ListGroupOffsetsOptions
   */
  public ListConsumerGroupOffsetsOptions topicPartitions(List<TopicPartition> topicPartitions) {
    this.topicPartitions = topicPartitions;
    return this;
  }

  /**
   * Returns a list of topic partitions to add as part of the result.
   */
  public List<TopicPartition> topicPartitions() {
    return topicPartitions;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    ListConsumerGroupOffsetsOptionsConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {
    return "ListConsumerGroupOffsetsOptions{" +
      "topicPartitions=" + topicPartitions +
      '}';
  }

}

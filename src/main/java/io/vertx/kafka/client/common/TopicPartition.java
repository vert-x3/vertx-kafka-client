package io.vertx.kafka.client.common;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@DataObject
public class TopicPartition {

  private String topic;
  private int partition;

  /**
   * Constructor
   */
  public TopicPartition() {
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public TopicPartition(JsonObject json) {
    this.topic = json.getString("topic");
    this.partition = json.getInteger("partition");
  }

  /**
   * Constructor (copy)
   *
   * @param that  object to copy
   */
  public TopicPartition(TopicPartition that) {
    this.topic = that.topic;
    this.partition = that.partition;
  }

  /**
   * The topic name
   *
   * @return
   */
  public String topic() {
    return this.topic;
  }

  /**
   * Set the topic name
   *
   * @param topic the topic name
   * @return  current instance of the class to be fluent
   */
  public TopicPartition setTopic(String topic) {
    this.topic = topic;
    return this;
  }

  /**
   * The partition number
   *
   * @return
   */
  public int partition() {
    return this.partition;
  }

  /**
   * Set the partition number
   *
   * @param partition the partition number
   * @return  current instance of the class to be fluent
   */
  public TopicPartition setPartition(int partition) {
    this.partition = partition;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {
    return new JsonObject().put("topic", this.topic).put("partition", this.partition);
  }
}

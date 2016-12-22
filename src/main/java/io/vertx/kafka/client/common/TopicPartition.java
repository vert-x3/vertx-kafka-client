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

  public TopicPartition() {
  }

  public TopicPartition(JsonObject json) {
    this.topic = json.getString("topic");
    this.partition = json.getInteger("partition");
  }

  public TopicPartition(TopicPartition that) {
    this.topic = that.topic;
    this.partition = that.partition;
  }

  public String topic() {
    return this.topic;
  }

  public TopicPartition setTopic(String topic) {
    this.topic = topic;
    return this;
  }

  public int partition() {
    return this.partition;
  }

  public TopicPartition setPartition(int partition) {
    this.partition = partition;
    return this;
  }

  public JsonObject toJson() {
    return new JsonObject().put("topic", this.topic).put("partition", this.partition);
  }
}

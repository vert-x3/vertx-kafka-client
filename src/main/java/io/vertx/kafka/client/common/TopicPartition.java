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
    topic = json.getString("topic");
    partition = json.getInteger("partition");
  }

  public TopicPartition(TopicPartition that) {
    topic = that.topic;
    partition = that.partition;
  }

  public String getTopic() {
    return topic;
  }

  public TopicPartition setTopic(String topic) {
    this.topic = topic;
    return this;
  }

  public int getPartition() {
    return partition;
  }

  public TopicPartition setPartition(int partition) {
    this.partition = partition;
    return this;
  }

  public JsonObject toJson() {
    return new JsonObject().put("topic", topic).put("partition", partition);
  }
}

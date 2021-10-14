package io.vertx.kafka.admin;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.common.TopicPartition;

@DataObject(generateConverter = true)
public class TopicPartitionReplicaInfo {
  private TopicPartition topicPartition;
  private ReplicaInfo replicaInfo;

  public TopicPartitionReplicaInfo() {
  }

  public TopicPartitionReplicaInfo(TopicPartition topicPartition, ReplicaInfo replicaInfo) {
    this.topicPartition = topicPartition;
    this.replicaInfo = replicaInfo;
  }

  public TopicPartitionReplicaInfo(JsonObject json) {
    TopicPartitionReplicaInfoConverter.fromJson(json, this);
  }

  public TopicPartition getTopicPartition() {
    return topicPartition;
  }

  public TopicPartitionReplicaInfo setTopicPartition(TopicPartition topicPartition) {
    this.topicPartition = topicPartition;
    return this;
  }

  public ReplicaInfo getReplicaInfo() {
    return replicaInfo;
  }

  public TopicPartitionReplicaInfo setReplicaInfo(ReplicaInfo replicaInfo) {
    this.replicaInfo = replicaInfo;
    return this;
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    TopicPartitionReplicaInfoConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {
    return "TopicPartitionReplicaInfo{" +
      "topicPartition=" + topicPartition +
      ", replicaInfo=" + replicaInfo +
      '}';
  }
}

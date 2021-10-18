package io.vertx.kafka.admin;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.List;

@DataObject(generateConverter = true)
public class LogDirInfo {
  private String path;
  private List<TopicPartitionReplicaInfo> replicaInfos;

  public LogDirInfo() {
  }

  public LogDirInfo(String path, List<TopicPartitionReplicaInfo> replicaInfos) {
    this.path = path;
    this.replicaInfos = replicaInfos;
  }

  public LogDirInfo(JsonObject json) {
    LogDirInfoConverter.fromJson(json, this);
  }

  public String getPath() {
    return path;
  }

  public LogDirInfo setPath(String path) {
    this.path = path;
    return this;
  }

  public List<TopicPartitionReplicaInfo> getReplicaInfos() {
    return replicaInfos;
  }

  public LogDirInfo setReplicaInfos(List<TopicPartitionReplicaInfo> replicaInfos) {
    this.replicaInfos = replicaInfos;
    return this;
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    LogDirInfoConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {
    return "LogDirInfo{" +
      "path='" + path + '\'' +
      ", replicaInfos=" + replicaInfos +
      '}';
  }
}

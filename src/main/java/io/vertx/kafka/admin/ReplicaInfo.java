package io.vertx.kafka.admin;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject
public class ReplicaInfo {
  private long size;
  private long offsetLag;
  private boolean isFuture;

  public ReplicaInfo() {
  }

  public ReplicaInfo(long size, long offsetLag, boolean isFuture) {
    this.size = size;
    this.offsetLag = offsetLag;
    this.isFuture = isFuture;
  }

  public ReplicaInfo(JsonObject json) {
    this.size = json.getLong("size");
    this.offsetLag = json.getLong("offsetLag");
    this.isFuture = json.getBoolean("isFuture");
  }

  public long getSize() {
    return size;
  }

  public ReplicaInfo setSize(long size) {
    this.size = size;
    return this;
  }

  public long getOffsetLag() {
    return offsetLag;
  }

  public ReplicaInfo setOffsetLag(long offsetLag) {
    this.offsetLag = offsetLag;
    return this;
  }

  public boolean isFuture() {
    return isFuture;
  }

  public ReplicaInfo setFuture(boolean future) {
    isFuture = future;
    return this;
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    json
      .put("size", this.size)
      .put("offsetLag", this.offsetLag)
      .put("isFuture", this.isFuture);
    return json;
  }

  @Override
  public String toString() {
    return "ReplicaInfo{" +
      "size=" + size +
      ", offsetLag=" + offsetLag +
      ", isFuture=" + isFuture +
      '}';
  }
}

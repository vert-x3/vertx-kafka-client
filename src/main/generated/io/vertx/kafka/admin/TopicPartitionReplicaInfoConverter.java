package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.kafka.admin.TopicPartitionReplicaInfo}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.TopicPartitionReplicaInfo} original class using Vert.x codegen.
 */
public class TopicPartitionReplicaInfoConverter {


  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, TopicPartitionReplicaInfo obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "replicaInfo":
          if (member.getValue() instanceof JsonObject) {
            obj.setReplicaInfo(new io.vertx.kafka.admin.ReplicaInfo((io.vertx.core.json.JsonObject)member.getValue()));
          }
          break;
      }
    }
  }

  public static void toJson(TopicPartitionReplicaInfo obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(TopicPartitionReplicaInfo obj, java.util.Map<String, Object> json) {
    if (obj.getReplicaInfo() != null) {
      json.put("replicaInfo", obj.getReplicaInfo().toJson());
    }
  }
}

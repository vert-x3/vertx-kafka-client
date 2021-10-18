package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.kafka.admin.LogDirInfo}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.LogDirInfo} original class using Vert.x codegen.
 */
public class LogDirInfoConverter {


  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, LogDirInfo obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "path":
          if (member.getValue() instanceof String) {
            obj.setPath((String)member.getValue());
          }
          break;
        case "replicaInfos":
          if (member.getValue() instanceof JsonArray) {
            java.util.ArrayList<io.vertx.kafka.admin.TopicPartitionReplicaInfo> list =  new java.util.ArrayList<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof JsonObject)
                list.add(new io.vertx.kafka.admin.TopicPartitionReplicaInfo((io.vertx.core.json.JsonObject)item));
            });
            obj.setReplicaInfos(list);
          }
          break;
      }
    }
  }

  public static void toJson(LogDirInfo obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(LogDirInfo obj, java.util.Map<String, Object> json) {
    if (obj.getPath() != null) {
      json.put("path", obj.getPath());
    }
    if (obj.getReplicaInfos() != null) {
      JsonArray array = new JsonArray();
      obj.getReplicaInfos().forEach(item -> array.add(item.toJson()));
      json.put("replicaInfos", array);
    }
  }
}

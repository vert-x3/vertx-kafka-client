package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.kafka.admin.NewTopic}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.NewTopic} original class using Vert.x codegen.
 */
public class NewTopicConverter {


  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, NewTopic obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "config":
          if (member.getValue() instanceof JsonObject) {
            java.util.Map<String, java.lang.String> map = new java.util.LinkedHashMap<>();
            ((Iterable<java.util.Map.Entry<String, Object>>)member.getValue()).forEach(entry -> {
              if (entry.getValue() instanceof String)
                map.put(entry.getKey(), (String)entry.getValue());
            });
            obj.setConfig(map);
          }
          break;
        case "name":
          if (member.getValue() instanceof String) {
            obj.setName((String)member.getValue());
          }
          break;
        case "numPartitions":
          if (member.getValue() instanceof Number) {
            obj.setNumPartitions(((Number)member.getValue()).intValue());
          }
          break;
        case "replicationFactor":
          if (member.getValue() instanceof Number) {
            obj.setReplicationFactor(((Number)member.getValue()).shortValue());
          }
          break;
      }
    }
  }

  public static void toJson(NewTopic obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(NewTopic obj, java.util.Map<String, Object> json) {
    if (obj.getConfig() != null) {
      JsonObject map = new JsonObject();
      obj.getConfig().forEach((key, value) -> map.put(key, value));
      json.put("config", map);
    }
    if (obj.getName() != null) {
      json.put("name", obj.getName());
    }
    json.put("numPartitions", obj.getNumPartitions());
    json.put("replicationFactor", obj.getReplicationFactor());
  }
}

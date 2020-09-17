package io.vertx.kafka.client.common;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.kafka.client.common.ConfigResource}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.client.common.ConfigResource} original class using Vert.x codegen.
 */
public class ConfigResourceConverter {


  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, ConfigResource obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "default":
          if (member.getValue() instanceof Boolean) {
            obj.setDefault((Boolean)member.getValue());
          }
          break;
        case "name":
          if (member.getValue() instanceof String) {
            obj.setName((String)member.getValue());
          }
          break;
        case "type":
          if (member.getValue() instanceof String) {
            obj.setType(org.apache.kafka.common.config.ConfigResource.Type.valueOf((String)member.getValue()));
          }
          break;
      }
    }
  }

  public static void toJson(ConfigResource obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(ConfigResource obj, java.util.Map<String, Object> json) {
    json.put("default", obj.isDefault());
    if (obj.getName() != null) {
      json.put("name", obj.getName());
    }
    if (obj.getType() != null) {
      json.put("type", obj.getType().name());
    }
  }
}

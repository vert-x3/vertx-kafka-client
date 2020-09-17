package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.kafka.admin.ConfigSynonym}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.ConfigSynonym} original class using Vert.x codegen.
 */
public class ConfigSynonymConverter {


  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, ConfigSynonym obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "name":
          if (member.getValue() instanceof String) {
            obj.setName((String)member.getValue());
          }
          break;
        case "source":
          if (member.getValue() instanceof String) {
            obj.setSource(org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.valueOf((String)member.getValue()));
          }
          break;
        case "value":
          if (member.getValue() instanceof String) {
            obj.setValue((String)member.getValue());
          }
          break;
      }
    }
  }

  public static void toJson(ConfigSynonym obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(ConfigSynonym obj, java.util.Map<String, Object> json) {
    if (obj.getName() != null) {
      json.put("name", obj.getName());
    }
    if (obj.getSource() != null) {
      json.put("source", obj.getSource().name());
    }
    if (obj.getValue() != null) {
      json.put("value", obj.getValue());
    }
  }
}

package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.kafka.admin.ConfigSynonym}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.ConfigSynonym} original class using Vert.x codegen.
 */
public class ConfigSynonymConverter {

   static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, ConfigSynonym obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "name":
          if (member.getValue() instanceof String) {
            obj.setName((String)member.getValue());
          }
          break;
        case "value":
          if (member.getValue() instanceof String) {
            obj.setValue((String)member.getValue());
          }
          break;
        case "source":
          if (member.getValue() instanceof String) {
            obj.setSource(org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.valueOf((String)member.getValue()));
          }
          break;
      }
    }
  }

   static void toJson(ConfigSynonym obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

   static void toJson(ConfigSynonym obj, java.util.Map<String, Object> json) {
    if (obj.getName() != null) {
      json.put("name", obj.getName());
    }
    if (obj.getValue() != null) {
      json.put("value", obj.getValue());
    }
    if (obj.getSource() != null) {
      json.put("source", obj.getSource().name());
    }
  }
}

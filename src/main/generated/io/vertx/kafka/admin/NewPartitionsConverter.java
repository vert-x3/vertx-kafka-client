package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.kafka.admin.NewPartitions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.NewPartitions} original class using Vert.x codegen.
 */
public class NewPartitionsConverter {

   static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, NewPartitions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "totalCount":
          if (member.getValue() instanceof Number) {
            obj.setTotalCount(((Number)member.getValue()).intValue());
          }
          break;
      }
    }
  }

   static void toJson(NewPartitions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

   static void toJson(NewPartitions obj, java.util.Map<String, Object> json) {
    json.put("totalCount", obj.getTotalCount());
  }
}

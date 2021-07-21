package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter for {@link io.vertx.kafka.admin.OffsetSpec}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.OffsetSpec} original class using Vert.x codegen.
 */
public class OffsetSpecConverter {

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, OffsetSpec obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "spec":
          if (member.getValue() instanceof Number) {
            obj.setSpec(((Number)member.getValue()).longValue());
          }
          break;
      }
    }
  }

  public static void toJson(OffsetSpec obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(OffsetSpec obj, java.util.Map<String, Object> json) {
    json.put("spec", obj.getSpec());
  }
}

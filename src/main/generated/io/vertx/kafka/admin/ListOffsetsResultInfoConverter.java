package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter for {@link io.vertx.kafka.admin.ListOffsetsResultInfo}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.ListOffsetsResultInfo} original class using Vert.x codegen.
 */
public class ListOffsetsResultInfoConverter {

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, ListOffsetsResultInfo obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "offset":
          if (member.getValue() instanceof Number) {
            obj.setOffset(((Number)member.getValue()).longValue());
          }
          break;
        case "timestamp":
          if (member.getValue() instanceof Number) {
            obj.setTimestamp(((Number)member.getValue()).longValue());
          }
          break;
      }
    }
  }

  public static void toJson(ListOffsetsResultInfo obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(ListOffsetsResultInfo obj, java.util.Map<String, Object> json) {
    json.put("offset", obj.getOffset());
    json.put("timestamp", obj.getTimestamp());
  }
}

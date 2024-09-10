package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/**
 * Converter and mapper for {@link io.vertx.kafka.admin.ListOffsetsResultInfo}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.ListOffsetsResultInfo} original class using Vert.x codegen.
 */
public class ListOffsetsResultInfoConverter {

  private static final Base64.Decoder BASE64_DECODER = Base64.getUrlDecoder();
  private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();

   static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, ListOffsetsResultInfo obj) {
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
        case "leaderEpoch":
          if (member.getValue() instanceof Number) {
            obj.setLeaderEpoch(((Number)member.getValue()).intValue());
          }
          break;
      }
    }
  }

   static void toJson(ListOffsetsResultInfo obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

   static void toJson(ListOffsetsResultInfo obj, java.util.Map<String, Object> json) {
    json.put("offset", obj.getOffset());
    json.put("timestamp", obj.getTimestamp());
    if (obj.getLeaderEpoch() != null) {
      json.put("leaderEpoch", obj.getLeaderEpoch());
    }
  }
}

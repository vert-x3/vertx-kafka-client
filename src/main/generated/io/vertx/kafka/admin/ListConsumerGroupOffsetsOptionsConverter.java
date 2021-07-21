package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter for {@link io.vertx.kafka.admin.ListConsumerGroupOffsetsOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.ListConsumerGroupOffsetsOptions} original class using Vert.x codegen.
 */
public class ListConsumerGroupOffsetsOptionsConverter {

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, ListConsumerGroupOffsetsOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
      }
    }
  }

  public static void toJson(ListConsumerGroupOffsetsOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(ListConsumerGroupOffsetsOptions obj, java.util.Map<String, Object> json) {
  }
}

package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

/**
 * Converter and mapper for {@link io.vertx.kafka.admin.ListConsumerGroupOffsetsSpec}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.ListConsumerGroupOffsetsSpec} original class using Vert.x codegen.
 */
public class ListConsumerGroupOffsetsSpecConverter {

   static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, ListConsumerGroupOffsetsSpec obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
      }
    }
  }

   static void toJson(ListConsumerGroupOffsetsSpec obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

   static void toJson(ListConsumerGroupOffsetsSpec obj, java.util.Map<String, Object> json) {
  }
}

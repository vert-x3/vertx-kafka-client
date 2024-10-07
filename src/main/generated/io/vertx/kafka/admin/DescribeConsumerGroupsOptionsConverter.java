package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.kafka.admin.DescribeConsumerGroupsOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.DescribeConsumerGroupsOptions} original class using Vert.x codegen.
 */
public class DescribeConsumerGroupsOptionsConverter {

   static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, DescribeConsumerGroupsOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
      }
    }
  }

   static void toJson(DescribeConsumerGroupsOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

   static void toJson(DescribeConsumerGroupsOptions obj, java.util.Map<String, Object> json) {
  }
}

package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.kafka.admin.MemberAssignment}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.MemberAssignment} original class using Vert.x codegen.
 */
public class MemberAssignmentConverter {


  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, MemberAssignment obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "topicPartitions":
          if (member.getValue() instanceof JsonArray) {
            java.util.LinkedHashSet<io.vertx.kafka.client.common.TopicPartition> list =  new java.util.LinkedHashSet<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof JsonObject)
                list.add(new io.vertx.kafka.client.common.TopicPartition((io.vertx.core.json.JsonObject)item));
            });
            obj.setTopicPartitions(list);
          }
          break;
      }
    }
  }

  public static void toJson(MemberAssignment obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(MemberAssignment obj, java.util.Map<String, Object> json) {
    if (obj.getTopicPartitions() != null) {
      JsonArray array = new JsonArray();
      obj.getTopicPartitions().forEach(item -> array.add(item.toJson()));
      json.put("topicPartitions", array);
    }
  }
}

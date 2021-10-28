package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/**
 * Converter and mapper for {@link io.vertx.kafka.admin.TopicDescription}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.TopicDescription} original class using Vert.x codegen.
 */
public class TopicDescriptionConverter {


  private static final Base64.Decoder BASE64_DECODER = JsonUtil.BASE64_DECODER;
  private static final Base64.Encoder BASE64_ENCODER = JsonUtil.BASE64_ENCODER;

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, TopicDescription obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "internal":
          if (member.getValue() instanceof Boolean) {
            obj.setInternal((Boolean)member.getValue());
          }
          break;
        case "name":
          if (member.getValue() instanceof String) {
            obj.setName((String)member.getValue());
          }
          break;
        case "partitions":
          if (member.getValue() instanceof JsonArray) {
            java.util.ArrayList<io.vertx.kafka.client.common.TopicPartitionInfo> list =  new java.util.ArrayList<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof JsonObject)
                list.add(new io.vertx.kafka.client.common.TopicPartitionInfo((io.vertx.core.json.JsonObject)item));
            });
            obj.setPartitions(list);
          }
          break;
      }
    }
  }

  public static void toJson(TopicDescription obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(TopicDescription obj, java.util.Map<String, Object> json) {
    json.put("internal", obj.isInternal());
    if (obj.getName() != null) {
      json.put("name", obj.getName());
    }
    if (obj.getPartitions() != null) {
      JsonArray array = new JsonArray();
      obj.getPartitions().forEach(item -> array.add(item.toJson()));
      json.put("partitions", array);
    }
  }
}

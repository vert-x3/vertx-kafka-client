package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import io.vertx.core.spi.json.JsonCodec;

/**
 * Converter and Codec for {@link io.vertx.kafka.admin.MemberAssignment}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.MemberAssignment} original class using Vert.x codegen.
 */
public class MemberAssignmentConverter implements JsonCodec<MemberAssignment, JsonObject> {

  public static final MemberAssignmentConverter INSTANCE = new MemberAssignmentConverter();

  @Override public JsonObject encode(MemberAssignment value) { return (value != null) ? value.toJson() : null; }

  @Override public MemberAssignment decode(JsonObject value) { return (value != null) ? new MemberAssignment(value) : null; }

  @Override public Class<MemberAssignment> getTargetClass() { return MemberAssignment.class; }

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, MemberAssignment obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "topicPartitions":
          if (member.getValue() instanceof JsonArray) {
            java.util.LinkedHashSet<io.vertx.kafka.client.common.TopicPartition> list =  new java.util.LinkedHashSet<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof JsonObject)
                list.add(io.vertx.kafka.client.common.TopicPartitionConverter.INSTANCE.decode((JsonObject)item));
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
      obj.getTopicPartitions().forEach(item -> array.add(io.vertx.kafka.client.common.TopicPartitionConverter.INSTANCE.encode(item)));
      json.put("topicPartitions", array);
    }
  }
}

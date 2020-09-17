package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.kafka.admin.ConsumerGroupDescription}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.ConsumerGroupDescription} original class using Vert.x codegen.
 */
public class ConsumerGroupDescriptionConverter {


  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, ConsumerGroupDescription obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "coordinator":
          if (member.getValue() instanceof JsonObject) {
            obj.setCoordinator(new io.vertx.kafka.client.common.Node((io.vertx.core.json.JsonObject)member.getValue()));
          }
          break;
        case "groupId":
          if (member.getValue() instanceof String) {
            obj.setGroupId((String)member.getValue());
          }
          break;
        case "members":
          if (member.getValue() instanceof JsonArray) {
            java.util.ArrayList<io.vertx.kafka.admin.MemberDescription> list =  new java.util.ArrayList<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof JsonObject)
                list.add(new io.vertx.kafka.admin.MemberDescription((io.vertx.core.json.JsonObject)item));
            });
            obj.setMembers(list);
          }
          break;
        case "partitionAssignor":
          if (member.getValue() instanceof String) {
            obj.setPartitionAssignor((String)member.getValue());
          }
          break;
        case "simpleConsumerGroup":
          if (member.getValue() instanceof Boolean) {
            obj.setSimpleConsumerGroup((Boolean)member.getValue());
          }
          break;
        case "state":
          if (member.getValue() instanceof String) {
            obj.setState(org.apache.kafka.common.ConsumerGroupState.valueOf((String)member.getValue()));
          }
          break;
      }
    }
  }

  public static void toJson(ConsumerGroupDescription obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(ConsumerGroupDescription obj, java.util.Map<String, Object> json) {
    if (obj.getCoordinator() != null) {
      json.put("coordinator", obj.getCoordinator().toJson());
    }
    if (obj.getGroupId() != null) {
      json.put("groupId", obj.getGroupId());
    }
    if (obj.getMembers() != null) {
      JsonArray array = new JsonArray();
      obj.getMembers().forEach(item -> array.add(item.toJson()));
      json.put("members", array);
    }
    if (obj.getPartitionAssignor() != null) {
      json.put("partitionAssignor", obj.getPartitionAssignor());
    }
    json.put("simpleConsumerGroup", obj.isSimpleConsumerGroup());
    if (obj.getState() != null) {
      json.put("state", obj.getState().name());
    }
  }
}

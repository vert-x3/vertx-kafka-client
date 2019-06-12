package io.vertx.kafka.client.common;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import io.vertx.core.spi.json.JsonCodec;

/**
 * Converter and Codec for {@link io.vertx.kafka.client.common.TopicPartitionInfo}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.client.common.TopicPartitionInfo} original class using Vert.x codegen.
 */
public class TopicPartitionInfoConverter implements JsonCodec<TopicPartitionInfo, JsonObject> {

  public static final TopicPartitionInfoConverter INSTANCE = new TopicPartitionInfoConverter();

  @Override public JsonObject encode(TopicPartitionInfo value) { return (value != null) ? value.toJson() : null; }

  @Override public TopicPartitionInfo decode(JsonObject value) { return (value != null) ? new TopicPartitionInfo(value) : null; }

  @Override public Class<TopicPartitionInfo> getTargetClass() { return TopicPartitionInfo.class; }

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, TopicPartitionInfo obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "isr":
          if (member.getValue() instanceof JsonArray) {
            java.util.ArrayList<io.vertx.kafka.client.common.Node> list =  new java.util.ArrayList<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof JsonObject)
                list.add(io.vertx.kafka.client.common.NodeConverter.INSTANCE.decode((JsonObject)item));
            });
            obj.setIsr(list);
          }
          break;
        case "leader":
          if (member.getValue() instanceof JsonObject) {
            obj.setLeader(io.vertx.kafka.client.common.NodeConverter.INSTANCE.decode((JsonObject)member.getValue()));
          }
          break;
        case "partition":
          if (member.getValue() instanceof Number) {
            obj.setPartition(((Number)member.getValue()).intValue());
          }
          break;
        case "replicas":
          if (member.getValue() instanceof JsonArray) {
            java.util.ArrayList<io.vertx.kafka.client.common.Node> list =  new java.util.ArrayList<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof JsonObject)
                list.add(io.vertx.kafka.client.common.NodeConverter.INSTANCE.decode((JsonObject)item));
            });
            obj.setReplicas(list);
          }
          break;
      }
    }
  }

  public static void toJson(TopicPartitionInfo obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(TopicPartitionInfo obj, java.util.Map<String, Object> json) {
    if (obj.getIsr() != null) {
      JsonArray array = new JsonArray();
      obj.getIsr().forEach(item -> array.add(io.vertx.kafka.client.common.NodeConverter.INSTANCE.encode(item)));
      json.put("isr", array);
    }
    if (obj.getLeader() != null) {
      json.put("leader", io.vertx.kafka.client.common.NodeConverter.INSTANCE.encode(obj.getLeader()));
    }
    json.put("partition", obj.getPartition());
    if (obj.getReplicas() != null) {
      JsonArray array = new JsonArray();
      obj.getReplicas().forEach(item -> array.add(io.vertx.kafka.client.common.NodeConverter.INSTANCE.encode(item)));
      json.put("replicas", array);
    }
  }
}

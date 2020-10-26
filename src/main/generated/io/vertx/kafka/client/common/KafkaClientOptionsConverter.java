package io.vertx.kafka.client.common;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.kafka.client.common.KafkaClientOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.client.common.KafkaClientOptions} original class using Vert.x codegen.
 */
public class KafkaClientOptionsConverter {


  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, KafkaClientOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "config":
          if (member.getValue() instanceof JsonObject) {
            java.util.Map<String, java.lang.Object> map = new java.util.LinkedHashMap<>();
            ((Iterable<java.util.Map.Entry<String, Object>>)member.getValue()).forEach(entry -> {
              if (entry.getValue() instanceof Object)
                map.put(entry.getKey(), entry.getValue());
            });
            obj.setConfig(map);
          }
          break;
        case "tracePeerAddress":
          if (member.getValue() instanceof String) {
            obj.setTracePeerAddress((String)member.getValue());
          }
          break;
        case "tracingPolicy":
          if (member.getValue() instanceof String) {
            obj.setTracingPolicy(io.vertx.core.tracing.TracingPolicy.valueOf((String)member.getValue()));
          }
          break;
      }
    }
  }

  public static void toJson(KafkaClientOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(KafkaClientOptions obj, java.util.Map<String, Object> json) {
    if (obj.getConfig() != null) {
      JsonObject map = new JsonObject();
      obj.getConfig().forEach((key, value) -> map.put(key, value));
      json.put("config", map);
    }
    if (obj.getTracePeerAddress() != null) {
      json.put("tracePeerAddress", obj.getTracePeerAddress());
    }
    if (obj.getTracingPolicy() != null) {
      json.put("tracingPolicy", obj.getTracingPolicy().name());
    }
  }
}

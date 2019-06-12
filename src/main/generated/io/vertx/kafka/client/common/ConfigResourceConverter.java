package io.vertx.kafka.client.common;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import io.vertx.core.spi.json.JsonCodec;

/**
 * Converter and Codec for {@link io.vertx.kafka.client.common.ConfigResource}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.client.common.ConfigResource} original class using Vert.x codegen.
 */
public class ConfigResourceConverter implements JsonCodec<ConfigResource, JsonObject> {

  public static final ConfigResourceConverter INSTANCE = new ConfigResourceConverter();

  @Override public JsonObject encode(ConfigResource value) { return (value != null) ? value.toJson() : null; }

  @Override public ConfigResource decode(JsonObject value) { return (value != null) ? new ConfigResource(value) : null; }

  @Override public Class<ConfigResource> getTargetClass() { return ConfigResource.class; }

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, ConfigResource obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "default":
          if (member.getValue() instanceof Boolean) {
            obj.setDefault((Boolean)member.getValue());
          }
          break;
        case "name":
          if (member.getValue() instanceof String) {
            obj.setName((String)member.getValue());
          }
          break;
        case "type":
          if (member.getValue() instanceof String) {
            obj.setType(org.apache.kafka.common.config.ConfigResource.Type.valueOf((String)member.getValue()));
          }
          break;
      }
    }
  }

  public static void toJson(ConfigResource obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(ConfigResource obj, java.util.Map<String, Object> json) {
    json.put("default", obj.isDefault());
    if (obj.getName() != null) {
      json.put("name", obj.getName());
    }
    if (obj.getType() != null) {
      json.put("type", obj.getType().name());
    }
  }
}

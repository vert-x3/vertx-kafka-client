package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import io.vertx.core.spi.json.JsonCodec;

/**
 * Converter and Codec for {@link io.vertx.kafka.admin.MemberDescription}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.MemberDescription} original class using Vert.x codegen.
 */
public class MemberDescriptionConverter implements JsonCodec<MemberDescription, JsonObject> {

  public static final MemberDescriptionConverter INSTANCE = new MemberDescriptionConverter();

  @Override public JsonObject encode(MemberDescription value) { return (value != null) ? value.toJson() : null; }

  @Override public MemberDescription decode(JsonObject value) { return (value != null) ? new MemberDescription(value) : null; }

  @Override public Class<MemberDescription> getTargetClass() { return MemberDescription.class; }

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, MemberDescription obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "assignment":
          if (member.getValue() instanceof JsonObject) {
            obj.setAssignment(io.vertx.kafka.admin.MemberAssignmentConverter.INSTANCE.decode((JsonObject)member.getValue()));
          }
          break;
        case "clientId":
          if (member.getValue() instanceof String) {
            obj.setClientId((String)member.getValue());
          }
          break;
        case "consumerId":
          if (member.getValue() instanceof String) {
            obj.setConsumerId((String)member.getValue());
          }
          break;
        case "host":
          if (member.getValue() instanceof String) {
            obj.setHost((String)member.getValue());
          }
          break;
      }
    }
  }

  public static void toJson(MemberDescription obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(MemberDescription obj, java.util.Map<String, Object> json) {
    if (obj.getAssignment() != null) {
      json.put("assignment", io.vertx.kafka.admin.MemberAssignmentConverter.INSTANCE.encode(obj.getAssignment()));
    }
    if (obj.getClientId() != null) {
      json.put("clientId", obj.getClientId());
    }
    if (obj.getConsumerId() != null) {
      json.put("consumerId", obj.getConsumerId());
    }
    if (obj.getHost() != null) {
      json.put("host", obj.getHost());
    }
  }
}

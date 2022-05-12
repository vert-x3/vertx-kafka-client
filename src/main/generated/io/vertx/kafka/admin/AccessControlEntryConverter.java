package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/**
 * Converter and mapper for {@link io.vertx.kafka.admin.AccessControlEntry}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.AccessControlEntry} original class using Vert.x codegen.
 */
public class AccessControlEntryConverter {


  private static final Base64.Decoder BASE64_DECODER = JsonUtil.BASE64_DECODER;
  private static final Base64.Encoder BASE64_ENCODER = JsonUtil.BASE64_ENCODER;

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, AccessControlEntry obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
      }
    }
  }

  public static void toJson(AccessControlEntry obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(AccessControlEntry obj, java.util.Map<String, Object> json) {
  }
}

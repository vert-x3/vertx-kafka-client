package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.kafka.admin.Config}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.Config} original class using Vert.x codegen.
 */
public class ConfigConverter {


  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, Config obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "entries":
          if (member.getValue() instanceof JsonArray) {
            java.util.ArrayList<io.vertx.kafka.admin.ConfigEntry> list =  new java.util.ArrayList<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof JsonObject)
                list.add(new io.vertx.kafka.admin.ConfigEntry((io.vertx.core.json.JsonObject)item));
            });
            obj.setEntries(list);
          }
          break;
      }
    }
  }

  public static void toJson(Config obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(Config obj, java.util.Map<String, Object> json) {
    if (obj.getEntries() != null) {
      JsonArray array = new JsonArray();
      obj.getEntries().forEach(item -> array.add(item.toJson()));
      json.put("entries", array);
    }
  }
}

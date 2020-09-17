package io.vertx.kafka.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.kafka.admin.ClusterDescription}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.admin.ClusterDescription} original class using Vert.x codegen.
 */
public class ClusterDescriptionConverter {


  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, ClusterDescription obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "clusterId":
          if (member.getValue() instanceof String) {
            obj.setClusterId((String)member.getValue());
          }
          break;
        case "controller":
          if (member.getValue() instanceof JsonObject) {
            obj.setController(new io.vertx.kafka.client.common.Node((io.vertx.core.json.JsonObject)member.getValue()));
          }
          break;
        case "nodes":
          if (member.getValue() instanceof JsonArray) {
            java.util.ArrayList<io.vertx.kafka.client.common.Node> list =  new java.util.ArrayList<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof JsonObject)
                list.add(new io.vertx.kafka.client.common.Node((io.vertx.core.json.JsonObject)item));
            });
            obj.setNodes(list);
          }
          break;
      }
    }
  }

  public static void toJson(ClusterDescription obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(ClusterDescription obj, java.util.Map<String, Object> json) {
    if (obj.getClusterId() != null) {
      json.put("clusterId", obj.getClusterId());
    }
    if (obj.getController() != null) {
      json.put("controller", obj.getController().toJson());
    }
    if (obj.getNodes() != null) {
      JsonArray array = new JsonArray();
      obj.getNodes().forEach(item -> array.add(item.toJson()));
      json.put("nodes", array);
    }
  }
}

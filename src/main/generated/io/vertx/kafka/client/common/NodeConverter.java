package io.vertx.kafka.client.common;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import io.vertx.core.spi.json.JsonCodec;

/**
 * Converter and Codec for {@link io.vertx.kafka.client.common.Node}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.client.common.Node} original class using Vert.x codegen.
 */
public class NodeConverter implements JsonCodec<Node, JsonObject> {

  public static final NodeConverter INSTANCE = new NodeConverter();

  @Override public JsonObject encode(Node value) { return (value != null) ? value.toJson() : null; }

  @Override public Node decode(JsonObject value) { return (value != null) ? new Node(value) : null; }

  @Override public Class<Node> getTargetClass() { return Node.class; }
}

package io.vertx.kafka.client.common;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import io.vertx.core.spi.json.JsonCodec;

/**
 * Converter and Codec for {@link io.vertx.kafka.client.common.TopicPartition}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.client.common.TopicPartition} original class using Vert.x codegen.
 */
public class TopicPartitionConverter implements JsonCodec<TopicPartition, JsonObject> {

  public static final TopicPartitionConverter INSTANCE = new TopicPartitionConverter();

  @Override public JsonObject encode(TopicPartition value) { return (value != null) ? value.toJson() : null; }

  @Override public TopicPartition decode(JsonObject value) { return (value != null) ? new TopicPartition(value) : null; }

  @Override public Class<TopicPartition> getTargetClass() { return TopicPartition.class; }
}

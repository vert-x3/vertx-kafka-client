package io.vertx.kafka.client.consumer;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import io.vertx.core.spi.json.JsonCodec;

/**
 * Converter and Codec for {@link io.vertx.kafka.client.consumer.OffsetAndTimestamp}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.client.consumer.OffsetAndTimestamp} original class using Vert.x codegen.
 */
public class OffsetAndTimestampConverter implements JsonCodec<OffsetAndTimestamp, JsonObject> {

  public static final OffsetAndTimestampConverter INSTANCE = new OffsetAndTimestampConverter();

  @Override public JsonObject encode(OffsetAndTimestamp value) { return (value != null) ? value.toJson() : null; }

  @Override public OffsetAndTimestamp decode(JsonObject value) { return (value != null) ? new OffsetAndTimestamp(value) : null; }

  @Override public Class<OffsetAndTimestamp> getTargetClass() { return OffsetAndTimestamp.class; }
}

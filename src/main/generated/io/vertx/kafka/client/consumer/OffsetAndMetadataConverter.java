package io.vertx.kafka.client.consumer;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import io.vertx.core.spi.json.JsonCodec;

/**
 * Converter and Codec for {@link io.vertx.kafka.client.consumer.OffsetAndMetadata}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.client.consumer.OffsetAndMetadata} original class using Vert.x codegen.
 */
public class OffsetAndMetadataConverter implements JsonCodec<OffsetAndMetadata, JsonObject> {

  public static final OffsetAndMetadataConverter INSTANCE = new OffsetAndMetadataConverter();

  @Override public JsonObject encode(OffsetAndMetadata value) { return (value != null) ? value.toJson() : null; }

  @Override public OffsetAndMetadata decode(JsonObject value) { return (value != null) ? new OffsetAndMetadata(value) : null; }

  @Override public Class<OffsetAndMetadata> getTargetClass() { return OffsetAndMetadata.class; }
}

package io.vertx.kafka.client.producer;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import io.vertx.core.spi.json.JsonCodec;

/**
 * Converter and Codec for {@link io.vertx.kafka.client.producer.RecordMetadata}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.kafka.client.producer.RecordMetadata} original class using Vert.x codegen.
 */
public class RecordMetadataConverter implements JsonCodec<RecordMetadata, JsonObject> {

  public static final RecordMetadataConverter INSTANCE = new RecordMetadataConverter();

  @Override public JsonObject encode(RecordMetadata value) { return (value != null) ? value.toJson() : null; }

  @Override public RecordMetadata decode(JsonObject value) { return (value != null) ? new RecordMetadata(value) : null; }

  @Override public Class<RecordMetadata> getTargetClass() { return RecordMetadata.class; }
}

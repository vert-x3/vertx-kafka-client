package io.vertx.kafka.client;

import io.vertx.core.buffer.Buffer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class BufferDeserializer implements Deserializer<Buffer> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public Buffer deserialize(String topic, byte[] data) {
    return Buffer.buffer(data);
  }

  @Override
  public void close() {
  }
}

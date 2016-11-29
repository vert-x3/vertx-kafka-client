package io.vertx.kafka.client;

import io.vertx.core.buffer.Buffer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A registry for Kafka Serializers and Deserializers allowing to lookup serializers by class.
 * <p>
 * The {@link BufferSerializer} and {@link BufferDeserializer} are registered along with the out of the box Kafka
 * ones.
 *
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class KafkaCodecs {

  private static final ConcurrentMap<Class<?>, Serializer<?>> serializers = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Class<?>, Deserializer<?>> deserializers = new ConcurrentHashMap<>();

  static {
    serializers.put(Buffer.class, new BufferSerializer());
    serializers.put(Double.class, new DoubleSerializer());
    serializers.put(Integer.class, new IntegerSerializer());
    serializers.put(String.class, new StringSerializer());
    serializers.put(byte[].class, new ByteArraySerializer());
    serializers.put(Long.class, new LongSerializer());
    serializers.put(Bytes.class, new BytesSerializer());
    serializers.put(ByteBuffer.class, new ByteBufferSerializer());
  }

  static {
    deserializers.put(Buffer.class, new BufferDeserializer());
    deserializers.put(Double.class, new DoubleDeserializer());
    deserializers.put(Integer.class, new IntegerDeserializer());
    deserializers.put(String.class, new StringDeserializer());
    deserializers.put(byte[].class, new ByteArrayDeserializer());
    deserializers.put(Long.class, new LongDeserializer());
    deserializers.put(Bytes.class, new BytesDeserializer());
    deserializers.put(ByteBuffer.class, new ByteBufferDeserializer());
  }

  @SuppressWarnings("unchecked")
  public static <T> Serializer<T> serializer(Class<T> type) {
    return (Serializer<T>) serializers.get(type);
  }

  @SuppressWarnings("unchecked")
  public static <T> Deserializer<T> deserializer(Class<T> type) {
    return (Deserializer<T>) deserializers.get(type);
  }
}

/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vertx.kafka.client;

import io.vertx.core.Vertx;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A registry for Kafka Serializers and Deserializers allowing to lookup serializers by class.
 * <p>
 * The {@link BufferSerializer} and {@link BufferDeserializer} are registered along with the out of the box Kafka
 * ones.
 * <p>
 * Vert.x Kafka Client uses it for looking up classes to find {@link Serializer} and {@link Deserializer} classes
 * when using {@link io.vertx.kafka.client.consumer.KafkaConsumer#create(Vertx, Map, Class, Class)} or
 * {@link io.vertx.kafka.client.producer.KafkaProducer#create(Vertx, Map, Class, Class)}.
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

  /**
   * @return a serializer for the given {@code type}.
   */
  @SuppressWarnings("unchecked")
  public static <T> Serializer<T> serializer(Class<T> type) {
    return (Serializer<T>) serializers.get(type);
  }

  /**
   * @return a deserializer for the given {@code type}.
   */
  @SuppressWarnings("unchecked")
  public static <T> Deserializer<T> deserializer(Class<T> type) {
    return (Deserializer<T>) deserializers.get(type);
  }
}

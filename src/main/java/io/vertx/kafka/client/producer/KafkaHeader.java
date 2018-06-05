/*
 * Copyright 2018 Red Hat Inc.
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

package io.vertx.kafka.client.producer;

import io.vertx.codegen.annotations.CacheReturn;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.buffer.Buffer;
import io.vertx.kafka.client.producer.impl.KafkaHeaderImpl;

/**
 * Vert.x Kafka producer record header.
 */
@VertxGen
public interface KafkaHeader {

  static KafkaHeader header(String key, Buffer value) {
    return new KafkaHeaderImpl(key, value);
  }

  static KafkaHeader header(String key, String value) {
    return new KafkaHeaderImpl(key, value);
  }

  @GenIgnore
  static KafkaHeader header(String key, byte[] value) {
    return new KafkaHeaderImpl(key, Buffer.buffer(value));
  }

  /**
   * @return the buffer key
   */
  @CacheReturn
  String key();

  /**
   * @return the buffer value
   */
  @CacheReturn
  Buffer value();

}

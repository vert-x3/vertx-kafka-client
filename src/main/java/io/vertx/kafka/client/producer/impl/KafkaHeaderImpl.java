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

package io.vertx.kafka.client.producer.impl;

import io.vertx.core.buffer.Buffer;
import io.vertx.kafka.client.producer.KafkaHeader;

/**
 * Vert.x Kafka producer record header implementation
 */
public class KafkaHeaderImpl implements KafkaHeader {

  private String key;
  private Buffer value;

  public KafkaHeaderImpl(String key, Buffer value) {
    this.key = key;
    this.value = value;
  }

  public KafkaHeaderImpl(String key, String value) {
    this(key, Buffer.buffer(value));
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  public Buffer value() {
    return value;
  }

}

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

import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Vert.x Kafka producer record header implementation
 */
public class KafkaHeaderImpl implements KafkaHeader {

  private String key;
  private byte[] value;

  public KafkaHeaderImpl(String key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  public KafkaHeaderImpl(String key, String value) {
    this(key, value.getBytes());
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  public byte[] value() {
    return value;
  }

  /**
   * Convert {@link Headers} to a list of {@link KafkaHeader}
   *
   * @param headers Headers from the Apache Kafka Client
   * @return a list of {@link KafkaHeader}
   */
  public static List<KafkaHeader> fromHeaders(Headers headers) {

    if (headers == null || headers.toArray().length == 0) {
      return Collections.emptyList();
    }

    return Stream.of(headers.toArray()).map(header -> new KafkaHeaderImpl(header.key(), header.value())).collect(Collectors.toList());
  }

  /**
   * Convert a list of {@link KafkaHeader} to a list of {@link Header}
   *
   * @param kafkaHeaders list of {@link KafkaHeader}
   * @return a list of {@link Header} from the Apache Kafka Client
   */
  public static List<Header> toHeaderList(List<KafkaHeader> kafkaHeaders) {

    if (kafkaHeaders == null || kafkaHeaders.isEmpty()) {
      return Collections.emptyList();
    }

    return kafkaHeaders.stream().map(kafkaHeader -> new RecordHeader(kafkaHeader.key(), kafkaHeader.value())).collect(Collectors.toList());
  }
}

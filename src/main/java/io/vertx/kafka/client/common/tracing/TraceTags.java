/*
 * Copyright 2020 Red Hat Inc.
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
package io.vertx.kafka.client.common.tracing;

import io.vertx.core.spi.tracing.TagExtractor;

import java.util.function.Function;

/**
 * Tags for Kafka Tracing
 */
public enum TraceTags {
  // See https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
  SERVER_ADDRESS("server.address", q -> q.address),
  SERVER_PORT("server.port", q -> q.port),
  PEER_SERVICE("peer.service", q -> "kafka"),
  BUS_DESTINATION("messaging.destination.name", q -> q.topic);

  static final TagExtractor<TraceContext> TAG_EXTRACTOR = new TagExtractor<>() {
    private final TraceTags[] TAGS = TraceTags.values();

    @Override
    public int len(TraceContext obj) {
      return TAGS.length;
    }

    @Override
    public String name(TraceContext obj, int index) {
      return TAGS[index].name;
    }

    @Override
    public String value(TraceContext obj, int index) {
      return TAGS[index].fn.apply(obj);
    }
  };

  final String name;
  final Function<TraceContext, String> fn;

  TraceTags(String name, Function<TraceContext, String> fn) {
    this.name = name;
    this.fn = fn;
  }
}

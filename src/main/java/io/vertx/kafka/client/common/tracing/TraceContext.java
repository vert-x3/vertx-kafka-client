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

/**
 * TraceContext holds some context for tracing during a message writing / reading process
 */
class TraceContext {
  final String kind;
  final String address;
  final String port;
  final String topic;

  TraceContext(String kind, String address, String port, String topic) {
    this.kind = kind;
    this.address = address;
    this.port = port;
    this.topic = topic;
  }
}

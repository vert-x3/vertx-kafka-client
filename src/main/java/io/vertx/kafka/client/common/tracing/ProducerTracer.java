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

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.Context;
import io.vertx.core.spi.tracing.TagExtractor;
import io.vertx.core.spi.tracing.VertxTracer;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.kafka.client.common.KafkaClientOptions;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Utils;

/**
 * Tracer for Kafka producer, wrapping the generic tracer.
 */
public class ProducerTracer<S> {
  private final VertxTracer<Void, S> tracer;
  private final String address;
  private final String hostname;
  private final String port;
  private final TracingPolicy policy;

  public static <S> @Nullable ProducerTracer create(VertxTracer tracer, KafkaClientOptions opts) {
    TracingPolicy policy = opts.getTracingPolicy() != null ? opts.getTracingPolicy() : TracingPolicy.PROPAGATE;
    if (policy == TracingPolicy.IGNORE || tracer == null) {
      // No need to create a tracer if it won't be used
      return null;
    }
    return new ProducerTracer<S>(tracer, policy, opts.getTracePeerAddress());
  }

  private ProducerTracer(VertxTracer<Void, S> tracer, TracingPolicy policy, String bootstrapServer) {
    if (tracer == null) {
      throw new IllegalArgumentException("tracer should not be null");
    }
    this.tracer = tracer;
    this.address = bootstrapServer;
    this.hostname = Utils.getHost(bootstrapServer);
    Integer port = Utils.getPort(bootstrapServer);
    this.port = port == null ? null : port.toString();
    this.policy = policy;
  }

  public StartedSpan prepareSendMessage(Context context, ProducerRecord record) {
    TraceContext tc = new TraceContext("producer", address, hostname, port, record.topic());
    S span = tracer.sendRequest(context, policy, tc, "kafka_send", (k, v) -> record.headers().add(k, v.getBytes()), TraceTags.TAG_EXTRACTOR);
    return new StartedSpan(span);
  }

  public class StartedSpan {
    private final S span;

    private StartedSpan(S span) {
      this.span = span;
    }

    public void finish(Context context) {
      // We don't add any new tag to the span here, just stop span timer
      tracer.receiveResponse(context, null, span, null, TagExtractor.<TraceContext>empty());
    }

    public void fail(Context context, Throwable failure) {
      tracer.receiveResponse(context, null, span, failure, TagExtractor.empty());
    }
  }
}

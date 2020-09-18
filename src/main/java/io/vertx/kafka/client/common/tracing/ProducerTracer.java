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
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.spi.tracing.TagExtractor;
import io.vertx.core.spi.tracing.VertxTracer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Utils;

import java.util.function.BiFunction;

/**
 * Tracer for Kafka producer, wrapping the generic tracer.
 */
public class ProducerTracer<S> {
  private final VertxTracer<Void, S> tracer;
  private final String address;
  private final String hostname;
  private final String port;

  public static <S> @Nullable ProducerTracer create(Vertx vertx, BiFunction<String, String, String> mapOrDefault) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    if (ctx.tracer() == null) {
      return null;
    }
    return new ProducerTracer<S>(ctx.tracer(), mapOrDefault.apply(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ""));
  }

  private ProducerTracer(VertxTracer<Void, S> tracer, String bootstrapServer) {
    if (tracer == null) {
      throw new IllegalArgumentException("tracer should not be null");
    }
    this.tracer = tracer;
    this.address = bootstrapServer;
    this.hostname = Utils.getHost(bootstrapServer);
    Integer port = Utils.getPort(bootstrapServer);
    this.port = port == null ? null : port.toString();
  }

  public StartedSpan prepareSendMessage(Context context, ProducerRecord record) {
    TraceContext tc = new TraceContext("producer", address, hostname, port, record.topic());
    S span = tracer.sendRequest(context, tc, "kafka_send", (k, v) -> record.headers().add(k, v.getBytes()), TraceTags.TAG_EXTRACTOR);
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

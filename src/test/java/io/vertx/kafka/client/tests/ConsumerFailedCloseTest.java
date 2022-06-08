/*
 * Copyright 2022 Red Hat Inc.
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

package io.vertx.kafka.client.tests;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.KafkaClientOptions;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.consumer.impl.KafkaReadStreamImpl;
import org.apache.kafka.common.KafkaException;

import java.time.Duration;
import java.util.Properties;

public class ConsumerFailedCloseTest extends ConsumerTestBase {

  @Override
  <K, V> KafkaReadStream<K, V> createConsumer(Vertx vertx, Properties config) {
    return new KafkaReadStreamImpl<>(
      vertx,
      new org.apache.kafka.clients.consumer.KafkaConsumer<K, V>(config) {
        @Override
        public void close(final Duration timeout) {
          super.close(timeout);
          throw new KafkaException("failed to close consumer");
        }
      },
      KafkaClientOptions.fromProperties(config, false));
  }
}

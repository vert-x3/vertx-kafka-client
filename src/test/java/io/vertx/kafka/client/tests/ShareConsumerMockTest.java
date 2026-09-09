/*
 * Copyright (c) 2011-2019 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package io.vertx.kafka.client.tests;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;

public class ShareConsumerMockTest extends ShareConsumerMockTestBase {

  @Override
  <K, V> KafkaShareConsumer<K, V> createShareConsumer(Vertx vertx, ShareConsumer<K, V> consumer) {
    return KafkaShareConsumer.create(vertx, consumer);
  }

}

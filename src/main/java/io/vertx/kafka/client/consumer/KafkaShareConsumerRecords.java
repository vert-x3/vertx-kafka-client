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
package io.vertx.kafka.client.consumer;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import org.apache.kafka.clients.consumer.ConsumerRecords;

@VertxGen
public interface KafkaShareConsumerRecords<K, V> {

  int size();

  boolean isEmpty();

  KafkaShareConsumerRecord<K, V> recordAt(int index);

  @GenIgnore
  ConsumerRecords<K, V> records();
}

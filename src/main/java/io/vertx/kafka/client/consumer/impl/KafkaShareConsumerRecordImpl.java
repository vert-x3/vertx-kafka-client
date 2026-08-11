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
package io.vertx.kafka.client.consumer.impl;

import io.vertx.kafka.client.consumer.KafkaShareConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaShareConsumerRecordImpl<K, V> extends KafkaConsumerRecordImpl<K, V> implements KafkaShareConsumerRecord<K, V> {

  public KafkaShareConsumerRecordImpl(ConsumerRecord<K, V> record) {
    super(record);
  }

  @Override
  public int deliveryCount() {
    return record().deliveryCount()
      .map(Short::intValue)
      .orElse(0);
  }

  @Override
  public String toString() {
    return "KafkaShareConsumerRecord{" +
      "topic=" + topic() +
      ",partition=" + partition() +
      ",offset=" + offset() +
      ",timestamp=" + timestamp() +
      ",key=" + key() +
      ",value=" + value() +
      ",headers=" + headers() +
      ",deliveryCount=" + deliveryCount() +
      "}";
  }
}

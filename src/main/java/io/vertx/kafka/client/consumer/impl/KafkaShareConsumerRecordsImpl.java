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
import io.vertx.kafka.client.consumer.KafkaShareConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;

public class KafkaShareConsumerRecordsImpl<K, V> implements KafkaShareConsumerRecords<K, V> {

  private final ConsumerRecords<K, V> records;
  private final List<KafkaShareConsumerRecord<K, V>> list;

  public KafkaShareConsumerRecordsImpl(ConsumerRecords<K, V> records) {
    this.records = records;
    this.list = new ArrayList<>();
    for (ConsumerRecord<K, V> record : records) {
      list.add(new KafkaShareConsumerRecordImpl<>(record));
    }
  }

  @Override
  public int size() {
    return list.size();
  }

  @Override
  public boolean isEmpty() {
    return list.isEmpty();
  }

  @Override
  public KafkaShareConsumerRecord<K, V> recordAt(int index) {
    return list.get(index);
  }

  @Override
  public ConsumerRecords<K, V> records() {
    return records;
  }
}

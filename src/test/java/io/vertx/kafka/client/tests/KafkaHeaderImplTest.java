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

package io.vertx.kafka.client.tests;

import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.impl.KafkaHeaderImpl;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class KafkaHeaderImplTest {

  @Test
  public void testfromHeaders_withNull() {
    // Given
    Headers headers = null;

    // When
    List<KafkaHeader> kafkaHeaders = KafkaHeaderImpl.fromHeaders(headers);

    // Then
    assertEquals(Collections.emptyList(), kafkaHeaders);
  }

  @Test
  public void testfromHeaders_withHeaders() {
    // Given
    Header recordHeader1 = new RecordHeader("key1", "value1".getBytes());
    Header recordHeader2 = new RecordHeader("key2", "value2".getBytes());
    Headers headers = new RecordHeaders(Arrays.asList(recordHeader1, recordHeader2));

    // When
    List<KafkaHeader> kafkaHeaders = KafkaHeaderImpl.fromHeaders(headers);

    // Then
    assertNotNull(kafkaHeaders);
    assertEquals(2, kafkaHeaders.size());

    KafkaHeader kafkaHeader1 = kafkaHeaders.get(0);
    assertEquals("key1", kafkaHeader1.key());
    assertEquals("value1", new String(kafkaHeader1.value()));

    KafkaHeader kafkaHeader2 = kafkaHeaders.get(1);
    assertEquals("key2", kafkaHeader2.key());
    assertEquals("value2", new String(kafkaHeader2.value()));
  }

  @Test
  public void testtoHeaderList_withEmptyList() {
    // Given
    List<KafkaHeader> kafkaHeaders = Collections.emptyList();

    // When
    List<Header> headers = KafkaHeaderImpl.toHeaderList(kafkaHeaders);

    // Then
    assertEquals(Collections.emptyList(), headers);
  }

  @Test
  public void testtoHeaderList_withKafkaHeaders() {
    // Given
    KafkaHeader kafkaHeader1 = new KafkaHeaderImpl("key1", "value1");
    KafkaHeader kafkaHeader2 = new KafkaHeaderImpl("key2", "value2".getBytes());
    List<KafkaHeader> kafkaHeaders = Arrays.asList(kafkaHeader1, kafkaHeader2);

    // When
    List<Header> headers = KafkaHeaderImpl.toHeaderList(kafkaHeaders);

    // Then
    assertNotNull(headers);
    assertEquals(2, headers.size());

    Header header1 = headers.get(0);
    assertEquals("key1", header1.key());
    assertEquals("value1", new String(header1.value()));

    Header header2 = headers.get(1);
    assertEquals("key2", header2.key());
    assertEquals("value2", new String(header2.value()));
  }
}

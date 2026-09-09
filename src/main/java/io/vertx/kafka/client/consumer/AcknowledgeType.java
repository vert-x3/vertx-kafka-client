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

import io.vertx.codegen.annotations.VertxGen;

/**
 * Indicates how a record delivered by a {@link KafkaShareConsumer} was processed.
 * <p>
 * This is the Vert.x counterpart of {@code org.apache.kafka.clients.consumer.AcknowledgeType},
 * exposed so that {@link KafkaShareConsumer#acknowledge} remains usable from every
 * language supported by Vert.x.
 */
@VertxGen
public enum AcknowledgeType {

  /**
   * The record was processed successfully and will not be redelivered.
   */
  ACCEPT,

  /**
   * The record was not processed. It is made available again for redelivery,
   * possibly to another consumer of the share group.
   */
  RELEASE,

  /**
   * The record is unprocessable and will not be redelivered to any consumer
   * of the share group.
   */
  REJECT,

  /**
   * The record is still being processed. The acquisition lock is extended and
   * the record is returned again by the next {@link KafkaShareConsumer#poll} call.
   */
  RENEW
}

/*
 * Copyright 2019 Red Hat Inc.
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

package io.vertx.kafka.admin;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject(generateConverter = true)
public class OffsetSpec {
  public final static OffsetSpec EARLIEST = new OffsetSpec(-2L);
  public final static OffsetSpec LATEST = new OffsetSpec(-1L);
  public final static OffsetSpec TIMESTAMP(long timestamp) {
    return new OffsetSpec(timestamp);
  }

  private long spec;

  /**
   * Constructor
   *
   * @param spec the offset spec Spec.EARLIEST, Spec.LATEST, or a Spec.TIMESTAMP(long) value
   */
  public OffsetSpec(long spec) {
    this.spec = spec;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public OffsetSpec(JsonObject json) {
    OffsetSpecConverter.fromJson(json, this);
  }

  /**
   * @return offset spec
   */
  public long getSpec() {
    return spec;
  }

  /**
   * Set the offset spec
   *
   * @param spec the offset spec
   * @return current instance of the class to be fluent
   */
  public OffsetSpec setSpec(long spec) {
    this.spec = spec;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {

    JsonObject json = new JsonObject();
    OffsetSpecConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {
    String value;
    if (EARLIEST == this) {
      value = "EARLIEST";
    } else if (LATEST == this) {
      value = "LATEST";
    } else {
      value = String.format("TIMESTAMP(%d)", this.spec);
    }

    return "OffsetSpec{" +
      "spec=" + value +
      "}";
  }
}

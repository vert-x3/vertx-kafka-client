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

package io.vertx.kafka.client.common;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.common.config.ConfigResource.Type;

/**
 * A class representing resources that have configuration
 */
@DataObject
@JsonGen(publicConverter = false)
public class ConfigResource {

  private String name;
  private boolean isDefault;
  private Type type;

  /**
   * Constructor
   */
  public ConfigResource() {
  }

  /**
   * Constructor
   *
   * @param type a non-null resource type
   * @param name a non-null resource name
   */
  public ConfigResource(Type type,
                        java.lang.String name) {
    this.type = type;
    this.name = name;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public ConfigResource(JsonObject json) {

    ConfigResourceConverter.fromJson(json, this);
  }

  /**
   * @return the resource name
   */
  public String getName() {
    return name;
  }

  /**
   * Set the resource name
   *
   * @param name the resource name
   * @return current instance of the class to be fluent
   */
  public ConfigResource setName(String name) {
    this.name = name;
    return this;
  }

  /**
   * @return true if this is the default resource of a resource type. Resource name is empty for the default resource.
   */
  public boolean isDefault() {
    return isDefault;
  }

  /**
   * Set if this is the default resource of a resource type. Resource name is empty for the default resource.
   *
   * @param isDefault if this is the default resource of a resource type
   * @return current instance of the class to be fluent
   */
  public ConfigResource setDefault(boolean isDefault) {
    this.isDefault = isDefault;
    return this;
  }

  /**
   * @return the resource type
   */
  public Type getType() {
    return type;
  }

  /**
   * Set the resource type
   *
   * @param type the resource type
   * @return current instance of the class to be fluent
   */
  public ConfigResource setType(Type type) {
    this.type = type;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {

    JsonObject json = new JsonObject();
    ConfigResourceConverter.toJson(this, json);
    return json;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ConfigResource that = (ConfigResource) o;

    if (!name.equals(that.name)) return false;
    if (isDefault != that.isDefault) return false;
    return type == that.type;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + (name != null ? name.hashCode() : 0);
    result = 31 * result + (isDefault ? 1 : 0);
    result = 31 * result + type.ordinal();
    return result;
  }

  @Override
  public String toString() {

    return "ConfigResource{" +
      "name=" + this.name +
      ",type=" + this.type +
      ",isDefault=" + this.isDefault +
      "}";
  }
}

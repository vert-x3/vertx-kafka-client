/*
 * Copyright 2021 Red Hat Inc.
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

import java.util.Objects;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;
@DataObject(generateConverter = true)
public class AclBinding {
    private ResourcePattern pattern;
    private AccessControlEntry entry;


    /**
     * Constructor (from JSON representation)
     *
     * @param json  JSON representation
     */
    public AclBinding(JsonObject json) {
        AclBindingConverter.fromJson(json, this);
    }
    /**
     * Create an instance of this class with the provided parameters.
     *
     * @param pattern non-null resource pattern.
     * @param entry non-null entry
     */
    public AclBinding(ResourcePattern pattern, AccessControlEntry entry) {
        this.pattern = Objects.requireNonNull(pattern, "pattern");
        this.entry = Objects.requireNonNull(entry, "entry");
    }

    /**
     * @return the resource pattern for this binding.
     */
    public ResourcePattern pattern() {
        return pattern;
    }

    /**
     * @return the access control entry for this binding.
     */
    public final AccessControlEntry entry() {
        return entry;
    }

    /**
     * Convert object to JSON representation
     *
     * @return  JSON representation
     */
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        AclBindingConverter.toJson(this, json);
        return json;
    }

    @Override
    public String toString() {
        return "AclBinding{" +
        "pattern=" + pattern +
        "entry=" + entry +        
        '}';
    }
}
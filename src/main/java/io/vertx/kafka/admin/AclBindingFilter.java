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
public class AclBindingFilter {
    private ResourcePatternFilter patternFilter;
    private AccessControlEntryFilter entryFilter;

    /**
     * Constructor (from JSON representation)
     *
     * @param json  JSON representation
     */
    public AclBindingFilter(JsonObject json) {
        AclBindingFilterConverter.fromJson(json, this);
    }

    /**
     * A filter which matches any ACL binding.
     */
    public static final AclBindingFilter ANY = new AclBindingFilter(ResourcePatternFilter.ANY, AccessControlEntryFilter.ANY);

    /**
     * Create an instance of this filter with the provided parameters.
     *
     * @param patternFilter non-null pattern filter
     * @param entryFilter non-null access control entry filter
     */
    public AclBindingFilter(ResourcePatternFilter patternFilter, AccessControlEntryFilter entryFilter) {
        this.patternFilter = Objects.requireNonNull(patternFilter, "patternFilter");
        this.entryFilter = Objects.requireNonNull(entryFilter, "entryFilter");
    }

    /**
     * @return the resource pattern filter.
     */
    public ResourcePatternFilter patternFilter() {
        return patternFilter;
    }

    /**
     * @return the access control entry filter.
     */
    public final AccessControlEntryFilter entryFilter() {
        return entryFilter;
    }

    /**
     * Convert object to JSON representation
     *
     * @return  JSON representation
     */
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        AclBindingFilterConverter.toJson(this, json);
        return json;
    }

    @Override
    public String toString() {
        return "AclBindingFilter{" +
        "patternFilter=" + patternFilter +
        "entryFilter=" + entryFilter +        
        '}';
    }
}

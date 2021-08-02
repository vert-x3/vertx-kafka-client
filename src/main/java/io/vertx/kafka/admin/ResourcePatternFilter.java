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

public class ResourcePatternFilter {
    /**
     * Matches any resource pattern.
     */
    public static final ResourcePatternFilter ANY = new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY);

    private final ResourceType resourceType;
    private final String name;
    private final PatternType patternType;

    /**
     * Create a filter using the supplied parameters.
     *
     * @param resourceType non-null resource type.
     *                     If {@link ResourceType#ANY}, the filter will ignore the resource type of the pattern.
     *                     If any other resource type, the filter will match only patterns with the same type.
     * @param name         resource name or {@code null}.
     *                     If {@code null}, the filter will ignore the name of resources.
     *                     If {@link ResourcePattern#WILDCARD_RESOURCE}, will match only wildcard patterns.
     * @param patternType  non-null resource pattern type.
     *                     If {@link PatternType#ANY}, the filter will match patterns regardless of pattern type.
     *                     If {@link PatternType#MATCH}, the filter will match patterns that would match the supplied
     *                     {@code name}, including a matching prefixed and wildcards patterns.
     *                     If any other resource pattern type, the filter will match only patterns with the same type.
     */
    public ResourcePatternFilter(ResourceType resourceType, String name, PatternType patternType) {
        this.resourceType = Objects.requireNonNull(resourceType, "resourceType");
        this.name = name;
        this.patternType = Objects.requireNonNull(patternType, "patternType");
    }


    /**
     * @return the specific resource type this pattern matches
     */
    public ResourceType resourceType() {
        return resourceType;
    }

    /**
     * @return the resource name.
     */
    public String name() {
        return name;
    }

    /**
     * @return the resource pattern type.
     */
    public PatternType patternType() {
        return patternType;
    }
}

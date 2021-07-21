package io.vertx.kafka.admin;

import java.util.Objects;

public class ResourcePattern {
    /**
     * A special literal resource name that corresponds to 'all resources of a certain type'.
     */
    public static final String WILDCARD_RESOURCE = "*";

    private final ResourceType resourceType;
    private final String name;
    private final PatternType patternType;

    /**
     * Create a pattern using the supplied parameters.
     *
     * @param resourceType non-null, specific, resource type
     * @param name non-null resource name, which can be the {@link #WILDCARD_RESOURCE}.
     * @param patternType non-null, specific, resource pattern type, which controls how the pattern will match resource names.
     */
    public ResourcePattern(ResourceType resourceType, String name, PatternType patternType) {
        this.resourceType = Objects.requireNonNull(resourceType, "resourceType");
        this.name = Objects.requireNonNull(name, "name");
        this.patternType = Objects.requireNonNull(patternType, "patternType");

        if (resourceType == ResourceType.ANY) {
            throw new IllegalArgumentException("resourceType must not be ANY");
        }

        if (patternType == PatternType.MATCH || patternType == PatternType.ANY) {
            throw new IllegalArgumentException("patternType must not be " + patternType);
        }
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

    /**
     * @return a filter which matches only this pattern.
     */
    public ResourcePatternFilter toFilter() {
        return new ResourcePatternFilter(resourceType, name, patternType);
    }

    @Override
    public String toString() {
        return "ResourcePattern(resourceType=" + resourceType + ", name=" + ((name == null) ? "<any>" : name) + ", patternType=" + patternType + ")";
    }

    /**
     * @return {@code true} if this Resource has any UNKNOWN components.
     */
    public boolean isUnknown() {
        return resourceType.isUnknown() || patternType.isUnknown();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final ResourcePattern resource = (ResourcePattern) o;
        return resourceType == resource.resourceType &&
                Objects.equals(name, resource.name) &&
                patternType == resource.patternType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, name, patternType);
    }
}
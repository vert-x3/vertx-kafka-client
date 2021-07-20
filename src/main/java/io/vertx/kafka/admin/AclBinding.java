package io.vertx.kafka.admin;


import java.util.Objects;

public class AclBinding {
    private final ResourcePattern pattern;
    private final AccessControlEntry entry;

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
}
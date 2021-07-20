package io.vertx.kafka.admin;

import java.util.Objects;

public class AclBindingFilter {
    private final ResourcePatternFilter patternFilter;
    private final AccessControlEntryFilter entryFilter;

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
}

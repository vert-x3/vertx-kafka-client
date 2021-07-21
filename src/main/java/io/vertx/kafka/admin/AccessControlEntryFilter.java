package io.vertx.kafka.admin;

import java.util.Objects;

public class AccessControlEntryFilter {
    private final AccessControlEntryData data;

    /**
     * Matches any access control entry.
     */
    public static final AccessControlEntryFilter ANY =
            new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY);

    /**
     * Create an instance of an access control entry filter with the provided parameters.
     *
     * @param principal the principal or null
     * @param host the host or null
     * @param operation non-null operation
     * @param permissionType non-null permission type
     */
    public AccessControlEntryFilter(String principal, String host, AclOperation operation, AclPermissionType permissionType) {
        Objects.requireNonNull(operation);
        Objects.requireNonNull(permissionType);
        this.data = new AccessControlEntryData(principal, host, operation, permissionType);
    }

    /**
     * This is a non-public constructor used in AccessControlEntry#toFilter
     *
     * @param data     The access control data.
     */
    AccessControlEntryFilter(AccessControlEntryData data) {
        this.data = data;
    }

    /**
     * Return the principal or null.
     */
    public String principal() {
        return data.principal();
    }

    /**
     * Return the host or null. The value `*` means any host.
     */
    public String host() {
        return data.host();
    }

    /**
     * Return the AclOperation.
     */
    public AclOperation operation() {
        return data.operation();
    }

    /**
     * Return the AclPermissionType.
     */
    public AclPermissionType permissionType() {
        return data.permissionType();
    }

    @Override
    public String toString() {
        return data.toString();
    }

    /**
     * Return true if there are any UNKNOWN components.
     */
    public boolean isUnknown() {
        return data.isUnknown();
    }

    /**
     * Returns true if this filter could only match one ACE -- in other words, if
     * there are no ANY or UNKNOWN fields.
     */
    public boolean matchesAtMostOne() {
        return findIndefiniteField() == null;
    }

    /**
     * Returns a string describing an ANY or UNKNOWN field, or null if there is
     * no such field.
     */
    public String findIndefiniteField() {
        return data.findIndefiniteField();
    }
}

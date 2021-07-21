package io.vertx.kafka.admin;

class AccessControlEntryData {
    private final String principal;
    private final String host;
    private final AclOperation operation;
    private final AclPermissionType permissionType;

    AccessControlEntryData(String principal, String host, AclOperation operation, AclPermissionType permissionType) {
        this.principal = principal;
        this.host = host;
        this.operation = operation;
        this.permissionType = permissionType;
    }

    String principal() {
        return principal;
    }

    String host() {
        return host;
    }

    AclOperation operation() {
        return operation;
    }

    AclPermissionType permissionType() {
        return permissionType;
    }

    /**
     * Returns a string describing an ANY or UNKNOWN field, or null if there is
     * no such field.
     */
    public String findIndefiniteField() {
        if (principal() == null)
            return "Principal is NULL";
        if (host() == null)
            return "Host is NULL";
        if (operation() == AclOperation.ANY)
            return "Operation is ANY";
        if (operation() == AclOperation.UNKNOWN)
            return "Operation is UNKNOWN";
        if (permissionType() == AclPermissionType.ANY)
            return "Permission type is ANY";
        if (permissionType() == AclPermissionType.UNKNOWN)
            return "Permission type is UNKNOWN";
        return null;
    }

    /**
     * Return true if there are any UNKNOWN components.
     */
    boolean isUnknown() {
        return operation.isUnknown() || permissionType.isUnknown();
    }
}

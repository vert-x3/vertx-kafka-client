package io.vertx.kafka.admin;

public enum AclPermissionType {
    /**
     * Represents any AclPermissionType which this client cannot understand,
     * perhaps because this client is too old.
     */
    UNKNOWN((byte) 0),

    /**
     * In a filter, matches any AclPermissionType.
     */
    ANY((byte) 1),

    /**
     * Disallows access.
     */
    DENY((byte) 2),

    /**
     * Grants access.
     */
    ALLOW((byte) 3);

    private final byte code;

    AclPermissionType(byte code) {
        this.code = code;
    }

    /**
     * Return the code of this permission type.
     */
    public byte code() {
        return code;
    }

    /**
     * Return true if this permission type is UNKNOWN.
     */
    public boolean isUnknown() {
        return this == UNKNOWN;
    }
}
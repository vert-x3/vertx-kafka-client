package io.vertx.kafka.admin;

public enum AclOperation {
    /**
     * Represents any AclOperation which this client cannot understand, perhaps because this
     * client is too old.
     */
    UNKNOWN((byte) 0),

    /**
     * In a filter, matches any AclOperation.
     */
    ANY((byte) 1),

    /**
     * ALL operation.
     */
    ALL((byte) 2),

    /**
     * READ operation.
     */
    READ((byte) 3),

    /**
     * WRITE operation.
     */
    WRITE((byte) 4),

    /**
     * CREATE operation.
     */
    CREATE((byte) 5),

    /**
     * DELETE operation.
     */
    DELETE((byte) 6),

    /**
     * ALTER operation.
     */
    ALTER((byte) 7),

    /**
     * DESCRIBE operation.
     */
    DESCRIBE((byte) 8),

    /**
     * CLUSTER_ACTION operation.
     */
    CLUSTER_ACTION((byte) 9),

    /**
     * DESCRIBE_CONFIGS operation.
     */
    DESCRIBE_CONFIGS((byte) 10),

    /**
     * ALTER_CONFIGS operation.
     */
    ALTER_CONFIGS((byte) 11),

    /**
     * IDEMPOTENT_WRITE operation.
     */
    IDEMPOTENT_WRITE((byte) 12);

    private final byte code;

    AclOperation(byte code) {
        this.code = code;
    }

    /**
     * Return the code of this operation.
     */
    public byte code() {
        return code;
    }

    /**
     * Return true if this operation is UNKNOWN.
     */
    public boolean isUnknown() {
        return this == UNKNOWN;
    }
}

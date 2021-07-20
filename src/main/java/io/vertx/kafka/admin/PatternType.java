package io.vertx.kafka.admin;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum PatternType {
    /**
     * Represents any PatternType which this client cannot understand, perhaps because this client is too old.
     */
    UNKNOWN((byte) 0),

    /**
     * In a filter, matches any resource pattern type.
     */
    ANY((byte) 1),

    /**
     * In a filter, will perform pattern matching.
     *
     * e.g. Given a filter of {@code ResourcePatternFilter(TOPIC, "payments.received", MATCH)`}, the filter match
     * any ResourcePattern that matches topic 'payments.received'. This might include:
     * <ul>
     *     <li>A Literal pattern with the same type and name, e.g. {@code ResourcePattern(TOPIC, "payments.received", LITERAL)}</li>
     *     <li>A Wildcard pattern with the same type, e.g. {@code ResourcePattern(TOPIC, "*", LITERAL)}</li>
     *     <li>A Prefixed pattern with the same type and where the name is a matching prefix, e.g. {@code ResourcePattern(TOPIC, "payments.", PREFIXED)}</li>
     * </ul>
     */
    MATCH((byte) 2),

    /**
     * A literal resource name.
     *
     * A literal name defines the full name of a resource, e.g. topic with name 'foo', or group with name 'bob'.
     *
     * The special wildcard character {@code *} can be used to represent a resource with any name.
     */
    LITERAL((byte) 3),

    /**
     * A prefixed resource name.
     *
     * A prefixed name defines a prefix for a resource, e.g. topics with names that start with 'foo'.
     */
    PREFIXED((byte) 4);

    private final static Map<Byte, PatternType> CODE_TO_VALUE =
            Collections.unmodifiableMap(
                    Arrays.stream(PatternType.values())
                            .collect(Collectors.toMap(PatternType::code, Function.identity()))
            );

    private final static Map<String, PatternType> NAME_TO_VALUE =
            Collections.unmodifiableMap(
                    Arrays.stream(PatternType.values())
                            .collect(Collectors.toMap(PatternType::name, Function.identity()))
            );

    private final byte code;

    PatternType(byte code) {
        this.code = code;
    }

    /**
     * @return the code of this resource.
     */
    public byte code() {
        return code;
    }

    /**
     * @return whether this resource pattern type is UNKNOWN.
     */
    public boolean isUnknown() {
        return this == UNKNOWN;
    }

    /**
     * @return whether this resource pattern type is a concrete type, rather than UNKNOWN or one of the filter types.
     */
    public boolean isSpecific() {
        return this != UNKNOWN && this != ANY && this != MATCH;
    }
}

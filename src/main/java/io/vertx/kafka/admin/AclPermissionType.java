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

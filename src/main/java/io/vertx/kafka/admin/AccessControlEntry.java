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
import io.vertx.codegen.annotations.DataObject;

@DataObject(generateConverter = true)
public class AccessControlEntry {
    final AccessControlEntryData data;

    /**
     * Create an instance of an access control entry with the provided parameters.
     *
     * @param principal non-null principal
     * @param host non-null host
     * @param operation non-null operation, ANY is not an allowed operation
     * @param permissionType non-null permission type, ANY is not an allowed type
     */
    public AccessControlEntry(String principal, String host, AclOperation operation, AclPermissionType permissionType) {
        Objects.requireNonNull(principal);
        Objects.requireNonNull(host);
        Objects.requireNonNull(operation);
        if (operation == AclOperation.ANY)
            throw new IllegalArgumentException("operation must not be ANY");
        Objects.requireNonNull(permissionType);
        if (permissionType == AclPermissionType.ANY)
            throw new IllegalArgumentException("permissionType must not be ANY");
        this.data = new AccessControlEntryData(principal, host, operation, permissionType);
    }

    /**
     * Return the principal for this entry.
     */
    public String principal() {
        return data.principal();
    }

    /**
     * Return the host or `*` for all hosts.
     */
    public String host() {
        return data.host();
    }

    /**
     * Return the AclOperation. This method will never return AclOperation.ANY.
     */
    public AclOperation operation() {
        return data.operation();
    }

    /**
     * Return the AclPermissionType. This method will never return AclPermissionType.ANY.
     */
    public AclPermissionType permissionType() {
        return data.permissionType();
    }
}

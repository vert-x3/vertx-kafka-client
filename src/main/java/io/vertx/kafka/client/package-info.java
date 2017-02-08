/*
 * Copyright 2016 Red Hat Inc.
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

/**
 * = Vert.x Kafka client
 *
 * This component provides a Kafka client for reading and sending messages from/to an link:https://kafka.apache.org/[Apache Kafka] cluster.
 * From the consumer point of view, its API provides a bunch of methods for subscribing to a topic partition receiving
 * messages asynchronously or reading them as a stream (even with the possibility to pause the stream itself).
 * As producer, its API provides methods for sending message to a topic partition like writing on a stream.
 *
 * WARNING: this module has the tech preview status, this means the API can change between versions.
 *
 * == Using the Vert.x Kafka client
 *
 * As component not yet officially released in the Vert.x stack, to use the Vert.x Kafka client current snapshot version,
 * add the following repository under the _repositories_ section and the following dependency to the _dependencies_ section
 * of your build descriptor:
 *
 * * Maven (in your `pom.xml`):
 *
 * [source,xml,subs="+attributes"]
 * ----
 * <repository>
 *     <id>oss.sonatype.org-snapshot</id>
 *     <url>https://oss.sonatype.org/content/repositories/snapshots</url>
 * </repository>
 * ----
 *
 * [source,xml,subs="+attributes"]
 * ----
 * <dependency>
 *     <groupId>io.vertx</groupId>
 *     <artifactId>vertx-kafka-client</artifactId>
 *     <version>3.4.0-SNAPSHOT</version>
 * </dependency>
 * ----
 *
 * * Gradle (in your `build.gradle` file):
 *
 * [source,groovy,subs="+attributes"]
 * ----
 * maven { url "https://oss.sonatype.org/content/repositories/snapshots" }
 * ----
 *
 * [source,groovy,subs="+attributes"]
 * ----
 * compile io.vertx:vertx-kafka-client:3.4.0-SNAPSHOT
 * ----
 */
@Document(fileName = "index.adoc")
@ModuleGen(name = "vertx-kafka-client", groupPackage = "io.vertx")
package io.vertx.kafka.client;

import io.vertx.codegen.annotations.ModuleGen;
import io.vertx.docgen.Document;

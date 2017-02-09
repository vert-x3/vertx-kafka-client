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
 *
 * == Getting Started
 *
 * === Creating Kafka clients
 *
 * The creation of both clients, consumer and producer, is quite similar and it's strictly related on how it works using
 * the native Kafka client library. They need to be configured with a bunch of properties as described in the official
 * Apache Kafka documentation, for the link:https://kafka.apache.org/documentation/#newconsumerconfigs[consumer] and
 * for the link:https://kafka.apache.org/documentation/#producerconfigs[producer].
 * In order to do that, a {@link java.util.Properties} instance can be filled with such properties passing it to one of the
 * static creation methods exposed by {@link io.vertx.kafka.client.consumer.KafkaConsumer} and
 * {@link io.vertx.kafka.client.producer.KafkaProducer} interfaces. Another way is filling a {@link java.util.Map} instance
 * instead of the {@link java.util.Properties} one.
 * More advanced creation methods allow to specify the class type for the key and the value used for sending messages
 * or provided by received messages; this is a way for setting the key and value serializers/deserializers instead of
 * using the related properties for that.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example1}
 * ----
 *
 * In the above example, a {@link io.vertx.kafka.client.consumer.KafkaConsumer} instance is created using a {@link java.util.Properties}
 * instance in order to specify the Kafka nodes list to connect (just one) and the deserializers to use for getting key
 * and value from each received message.
 * The {@link io.vertx.kafka.client.producer.KafkaProducer} instance is created in a different way using a {@link java.util.Map}
 * instance for specifying Kafka nodes list to connect (just one) and the acknowledgment mode; the key and value
 * deserializers are specified as parameters in the
 * {@link io.vertx.kafka.client.producer.KafkaProducer#create(io.vertx.core.Vertx, java.util.Map, java.lang.Class, java.lang.Class)}
 * method.
 *
 * === Receiving messages from a topic joining a consumer group
 *
 * In order to start receiving messages from Kafka topics, the consumer can use the
 * {@link io.vertx.kafka.client.consumer.KafkaConsumer#subscribe(java.util.Set, io.vertx.core.Handler)} method for subscribing
 * to a set of topics being part of a consumer group (specified by the properties on creation) and being notified when the operation
 * is completed. Before doing that, it's mandatory to register an handler for handling incoming messages using the
 * {@link io.vertx.kafka.client.consumer.KafkaConsumer#handler(io.vertx.core.Handler)} otherwise an
 * {@link java.lang.IllegalStateException} will be thrown.
 *
 * Using the consumer group way, the Kafka cluster assigns partitions to the consumer taking into account other connected
 * consumers in the same consumer group, so that partitions can be spread across them. The Kafka cluster handles partitions re-balancing
 * when a consumer leaves the group (so assigned partitions are free to be assigned to other consumers) or a new consumer
 * joins the group (so it wants partitions to read from).
 * The {@link io.vertx.kafka.client.consumer.KafkaConsumer} interface provides a way for being notified
 * about what are the partitions revoked and assigned by the Kafka cluster specifying related handlers through the
 * {@link io.vertx.kafka.client.consumer.KafkaConsumer#partitionsRevokedHandler(io.vertx.core.Handler)} and the
 * {@link io.vertx.kafka.client.consumer.KafkaConsumer#partitionsAssignedHandler(io.vertx.core.Handler)}.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example2}
 * ----
 *
 * After joining a consumer group for receiving messages, a consumer can decide to leave the consumer group in order to
 * not get messages anymore. This is possible thanks to the {@link io.vertx.kafka.client.consumer.KafkaConsumer#unsubscribe(io.vertx.core.Handler)}
 * method.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example3}
 * ----
 *
 * === Receiving messages from a topic requesting specific partitions
 *
 * Other than being part of a consumer group for receiving messages from a topic, a consumer can ask for a specific
 * topic partition. The big difference is that without being part of a consumer group the overall application can't rely
 * on the re-balancing feature. The {@link io.vertx.kafka.client.consumer.KafkaConsumer} interface provides the
 * {@link io.vertx.kafka.client.consumer.KafkaConsumer#assign(java.util.Set, io.vertx.core.Handler)} method in order to
 * ask to be assigned specific partitions; using the {@link io.vertx.kafka.client.consumer.KafkaConsumer#assignment(io.vertx.core.Handler)}
 * method is also possible getting information about the current assigned partitions.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example4}
 * ----
 *
 * === Getting topic partitions information
 *
 * Both the {@link io.vertx.kafka.client.consumer.KafkaConsumer} and {@link io.vertx.kafka.client.producer.KafkaProducer}
 * interface provides the "partitionsFor" method for getting information about partitions in a specified topic.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example5}
 * ----
 *
 * The above example also shows that the {@link io.vertx.kafka.client.consumer.KafkaConsumer} interface provides one more
 * method for getting information about all available topics with related partitions.
 * This is the {@link io.vertx.kafka.client.consumer.KafkaConsumer#listTopics(io.vertx.core.Handler)} method which is not
 * available in the {@link io.vertx.kafka.client.producer.KafkaProducer} interface.
 */
@Document(fileName = "index.adoc")
@ModuleGen(name = "vertx-kafka-client", groupPackage = "io.vertx")
package io.vertx.kafka.client;

import io.vertx.codegen.annotations.ModuleGen;
import io.vertx.docgen.Document;

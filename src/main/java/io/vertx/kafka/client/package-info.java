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
 * :toc: left
 *
 * This component provides a Kafka client for reading and sending messages from/to an link:https://kafka.apache.org/[Apache Kafka] cluster.
 *
 * As consumer, the API provides methods for subscribing to a topic partition receiving
 * messages asynchronously or reading them as a stream (even with the possibility to pause/resume the stream).
 *
 * As producer, the API provides methods for sending message to a topic partition like writing on a stream.
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
 * == Creating Kafka clients
 *
 * Creating consumers and sproducer is quite similar and on how it works using the native Kafka client library.
 *
 * They need to be configured with a bunch of properties as described in the official
 * Apache Kafka documentation, for the link:https://kafka.apache.org/documentation/#newconsumerconfigs[consumer] and
 * for the link:https://kafka.apache.org/documentation/#producerconfigs[producer].
 *
 * To achieve that, a {@link java.util.Properties} instance can be configured with such properties passing it to one of the
 * static creation methods exposed by {@link io.vertx.kafka.client.consumer.KafkaConsumer} and
 * {@link io.vertx.kafka.client.producer.KafkaProducer}
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example1_0}
 * ----
 *
 * In the above example, a {@link io.vertx.kafka.client.consumer.KafkaConsumer} instance is created using
 * a {@link java.util.Properties} instance in order to specify the Kafka nodes list to connect (just one) and
 * the deserializers to use for getting key and value from each received message.
 *
 * Another way is to use a {@link java.util.Map} instance instead of the {@link java.util.Properties} which is available
 * only for the Java / Groovy / Kotlin
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example1_1}
 * ----
 *
 * More advanced creation methods allow to specify the class type for the key and the value used for sending messages
 * or provided by received messages; this is a way for setting the key and value serializers/deserializers instead of
 * using the related properties for that
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example1_2}
 * ----
 *
 * Here the {@link io.vertx.kafka.client.producer.KafkaProducer} instance is created in using a {@link java.util.Map} for
 * specifying Kafka nodes list to connect (just one) and the acknowledgment mode; the key and value deserializers are
 * specified as parameters of {@link io.vertx.kafka.client.producer.KafkaProducer#create(io.vertx.core.Vertx, java.util.Map, java.lang.Class, java.lang.Class)}.
 *
 * == Receiving messages from a topic joining a consumer group
 *
 * In order to start receiving messages from Kafka topics, the consumer can use the
 * {@link io.vertx.kafka.client.consumer.KafkaConsumer#subscribe(java.util.Set)} method for
 * subscribing to a set of topics being part of a consumer group (specified by the properties on creation).
 *
 * You need to register an handler for handling incoming messages using the
 * {@link io.vertx.kafka.client.consumer.KafkaConsumer#handler(io.vertx.core.Handler)}
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleSubscribe(io.vertx.kafka.client.consumer.KafkaConsumer)}
 * ----
 *
 * An handler can also be passed during subscription to be aware of the subscription result and being notified when the operation
 * is completed.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleSubscribeWithResult(io.vertx.kafka.client.consumer.KafkaConsumer)}
 * ----
 *
 * Using the consumer group way, the Kafka cluster assigns partitions to the consumer taking into account other connected
 * consumers in the same consumer group, so that partitions can be spread across them.
 *
 * The Kafka cluster handles partitions re-balancing when a consumer leaves the group (so assigned partitions are free
 * to be assigned to other consumers) or a new consumer joins the group (so it wants partitions to read from).
 *
 * You can register handlers on a {@link io.vertx.kafka.client.consumer.KafkaConsumer} to be notified
 * of the partitions revocations and assignments by the Kafka cluster using
 * {@link io.vertx.kafka.client.consumer.KafkaConsumer#partitionsRevokedHandler(io.vertx.core.Handler)} and
 * {@link io.vertx.kafka.client.consumer.KafkaConsumer#partitionsAssignedHandler(io.vertx.core.Handler)}.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example2}
 * ----
 *
 * After joining a consumer group for receiving messages, a consumer can decide to leave the consumer group in order to
 * not get messages anymore using {@link io.vertx.kafka.client.consumer.KafkaConsumer#unsubscribe()}
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleUnsubscribe}
 * ----
 *
 * You can add an handler to be notified of the result
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleUnsubscribeWithCallback}
 * ----
 *
 * == Receiving messages from a topic requesting specific partitions
 *
 * Besides being part of a consumer group for receiving messages from a topic, a consumer can ask for a specific
 * topic partition. When the consumer is not part part of a consumer group the overall application cannot
 * rely on the re-balancing feature.
 *
 * You can use {@link io.vertx.kafka.client.consumer.KafkaConsumer#assign(java.util.Set, io.vertx.core.Handler)}
 * in order to ask for specific partitions.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example4}
 * ----
 *
 * Calling {@link io.vertx.kafka.client.consumer.KafkaConsumer#assignment(io.vertx.core.Handler)} provides
 * the list of the current assigned partitions.
 *
 * == Getting topic partition information
 *
 * You can call the {@link io.vertx.kafka.client.consumer.KafkaConsumer#partitionsFor} to get information about
 * partitions for a specified topic
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleConsumerPartitionsFor}
 * ----
 *
 * In addition {@link io.vertx.kafka.client.consumer.KafkaConsumer#listTopics} provides all available topics
 * with related partitions
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleConsumerListTopics}
 * ----
 *
 * == Committing offset manually
 *
 * In Apache Kafka, one of the main features is that the consumer is in charge to handle the offset of the last read message.
 * This is executed by the commit operation that can be executed automatically every time a bunch of messages are read
 * from a topic partition; in this case the "enable.auto.commit" configuration parameter needs to be set to "true" in
 * the properties bag for the consumer creation.
 * The other way is using the {@link io.vertx.kafka.client.consumer.KafkaConsumer#commit(io.vertx.core.Handler)} method
 * in order to do that manually (it's useful for having an "at least once" delivery to be sure that the read messages
 * are processed before committing the offset).
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example6}
 * ----
 *
 * == Seeking in a topic partition
 *
 * A great advantage of using Apache Kafka is that the messages are retained for a long period of time and the consumer can
 * seek inside a topic partition for re-reading all or part of the messages and then coming back to the end of
 * the partition. Using the {@link io.vertx.kafka.client.consumer.KafkaConsumer#seek(io.vertx.kafka.client.common.TopicPartition, long, io.vertx.core.Handler)}
 * method it's possible to change the offset for starting to read at specific position. If the consumer needs to re-read the stream
 * from the beginning, there is the {@link io.vertx.kafka.client.consumer.KafkaConsumer#seekToBeginning(java.util.Set, io.vertx.core.Handler)}
 * method. Finally, in order to come back at the end of the partition, it's possible to use the
 * {@link io.vertx.kafka.client.consumer.KafkaConsumer#seekToEnd(java.util.Set, io.vertx.core.Handler)} method.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example7}
 * ----
 *
 * == Pausing and resuming the read on topic partitions
 *
 * A consumer has the possibility to pause the read operation from a topic, in order to not receive other messages
 * (i.e. having more time to process the messages already read) and then resume the read for continuing to receive messages.
 * In order to do that, the {@link io.vertx.kafka.client.consumer.KafkaConsumer} interface provides the
 * {@link io.vertx.kafka.client.consumer.KafkaConsumer#pause(java.util.Set, io.vertx.core.Handler)} method and the
 * {@link io.vertx.kafka.client.consumer.KafkaConsumer#resume(java.util.Set, io.vertx.core.Handler)} method.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example8}
 * ----
 *
 * == Sending messages to a topic
 *
 * The {@link io.vertx.kafka.client.producer.KafkaProducer} interface provides the
 * {@link io.vertx.kafka.client.producer.KafkaProducer#write(io.vertx.kafka.client.producer.KafkaProducerRecord, io.vertx.core.Handler)}
 * method for sending messages (records) to a topic having the possibility to receive metadata about the messages sent like
 * the topic itself, the destination partition and the assigned offset. The simpler way is sending a message specifying
 * only the destination topic and the related value; in this case, without a key or a specific partition, the sender works
 * in a round robin way sending messages across all the partitions of the topic.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example9}
 * ----
 *
 * In order to specify the destination partition for a message, it's possible to specify the partition identifier explicitly
 * or a key for the message.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example10}
 * ----
 *
 * Using a key, the sender processes an hash on that in order to identify the destination partition; it
 * guarantees that all messages with the same key are sent to the same partition in order.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example11}
 * ----
 *
 * == Getting topic partition information
 *
 * You can call the {@link io.vertx.kafka.client.producer.KafkaProducer#partitionsFor} to get information about
 * partitions for a specified topic:
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleProducerPartitionsFor}
 * ----
 *
 * == Handling exceptions and errors
 *
 * In order to handle potential errors and exceptions during the communication between a Kafka client (consumer or producer)
 * and the Kafka cluster, both {@link io.vertx.kafka.client.consumer.KafkaConsumer} and {@link io.vertx.kafka.client.producer.KafkaProducer}
 * interface provide the "exceptionHandler" method for setting an handler called when an error happens (i.e. timeout).
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#example12}
 * ----
 *
 * == Stream implementation and native Kafka objects
 *
 * Other than the polyglot version of the Kafka consumer and producer, this component provides a stream oriented
 * implementation which handles native Kafka objects (and not the related Vert.x counterparts).
 * The available interfaces are {@link io.vertx.kafka.client.consumer.KafkaReadStream} for reading topic partitions and
 * {@link io.vertx.kafka.client.producer.KafkaWriteStream} for writing to topics. The extends the interfaces provided
 * by Vert.x for handling stream so the {@link io.vertx.core.streams.ReadStream} and {@link io.vertx.core.streams.WriteStream}
 * where the handled classes are the native ones from the Kafka client libraries like the
 * {@link org.apache.kafka.clients.consumer.ConsumerRecord} and the {@link org.apache.kafka.clients.producer.ProducerRecord}.
 * The way to interact with the above streams is quite similar to the polyglot version.
 *
 */
@Document(fileName = "index.adoc")
@ModuleGen(name = "vertx-kafka-client", groupPackage = "io.vertx")
package io.vertx.kafka.client;

import io.vertx.codegen.annotations.ModuleGen;
import io.vertx.docgen.Document;

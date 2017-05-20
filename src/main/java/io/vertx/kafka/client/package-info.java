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
 * :lang: $lang
 * :$lang: $lang
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
 * <dependency>
 *   <groupId>io.vertx</groupId>
 *   <artifactId>vertx-kafka-client</artifactId>
 *   <version>${maven.version}</version>
 * </dependency>
 * ----
 *
 * * Gradle (in your `build.gradle` file):
 *
 * [source,groovy,subs="+attributes"]
 * ----
 * compile io.vertx:vertx-kafka-client:${maven.version}
 * ----
 *
 * == Creating Kafka clients
 *
 * Creating consumers and sproducers is quite similar and on how it works using the native Kafka client library.
 *
 * They need to be configured with a bunch of properties as described in the official
 * Apache Kafka documentation, for the link:https://kafka.apache.org/documentation/#newconsumerconfigs[consumer] and
 * for the link:https://kafka.apache.org/documentation/#producerconfigs[producer].
 *
 * To achieve that, a map can be configured with such properties passing it to one of the
 * static creation methods exposed by {@link io.vertx.kafka.client.consumer.KafkaConsumer} and
 * {@link io.vertx.kafka.client.producer.KafkaProducer}
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleCreateConsumer}
 * ----
 *
 * In the above example, a {@link io.vertx.kafka.client.consumer.KafkaConsumer} instance is created using
 * a map instance in order to specify the Kafka nodes list to connect (just one) and
 * the deserializers to use for getting key and value from each received message.
 *
 * Likewise a producer can be created
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#createProducer}
 * ----
 *
 * ifdef::java,groovy,kotlin[]
 * Another way is to use a {@link java.util.Properties} instance instead of the map.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleCreateConsumerJava}
 * ----
 *
 * More advanced creation methods allow to specify the class type for the key and the value used for sending messages
 * or provided by received messages; this is a way for setting the key and value serializers/deserializers instead of
 * using the related properties for that
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#createProducerJava}
 * ----
 *
 * Here the {@link io.vertx.kafka.client.producer.KafkaProducer} instance is created in using a {@link java.util.Properties} for
 * specifying Kafka nodes list to connect (just one) and the acknowledgment mode; the key and value deserializers are
 * specified as parameters of {@link io.vertx.kafka.client.producer.KafkaProducer#create(io.vertx.core.Vertx, java.util.Properties, java.lang.Class, java.lang.Class)}.
 * endif::[]
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
 * A handler can also be passed during subscription to be aware of the subscription result and being notified when the operation
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
 * {@link examples.VertxKafkaClientExamples#exampleConsumerPartitionsNotifs}
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
 * {@link examples.VertxKafkaClientExamples#exampleConsumerAssignPartition}
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
 * == Manual offset commit
 *
 * In Apache Kafka the consumer is in charge to handle the offset of the last read message.
 *
 * This is executed by the commit operation executed automatically every time a bunch of messages are read
 * from a topic partition. The configuration parameter `enable.auto.commit` must be set to `true` when the
 * consumer is created.
 *
 * Manual offset commit, can be achieved with {@link io.vertx.kafka.client.consumer.KafkaConsumer#commit(io.vertx.core.Handler)}.
 * It can be used to achieve _at least once_ delivery to be sure that the read messages are processed before committing
 * the offset.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleConsumerManualOffsetCommit}
 * ----
 *
 * == Seeking in a topic partition
 *
 * Apache Kafka can retain messages for a long period of time and the consumer can seek inside a topic partition
 * and obtain arbitrary access to the messages.
 *
 * You can use {@link io.vertx.kafka.client.consumer.KafkaConsumer#seek} to change the offset for reading at a specific
 * position
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleSeek}
 * ----
 *
 * When the consumer needs to re-read the stream from the beginning, it can use {@link io.vertx.kafka.client.consumer.KafkaConsumer#seekToBeginning}
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleSeekToBeginning}
 * ----
 *
 * Finally {@link io.vertx.kafka.client.consumer.KafkaConsumer#seekToEnd} can be used to come back at the end of the partition
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleSeekToEnd}
 * ----
 *
 * == Offset lookup
 *
 * You can use the beginningOffsets API introduced in Kafka 0.10.1.1 to get the first offset
 * for a given partition. In contrast to {@link io.vertx.kafka.client.consumer.KafkaConsumer#seekToBeginning},
 * it does not change the consumer's offset.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleConsumerBeginningOffsets}
 * ----
 *
 * You can use the endOffsets API introduced in Kafka 0.10.1.1 to get the last offset
 * for a given partition. In contrast to {@link io.vertx.kafka.client.consumer.KafkaConsumer#seekToEnd},
 * it does not change the consumer's offset.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleConsumerEndOffsets}
 * ----
 *
 * You can use the offsetsForTimes API introduced in Kafka 0.10.1.1 to look up an offset by
 * timestamp, i.e. search parameter is an epoch timestamp and the call returns the lowest offset
 * with ingestion timestamp >= given timestamp.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleConsumerOffsetsForTimes}
 * ----
 * == Message flow control
 *
 * A consumer can control the incoming message flow and pause/resume the read operation from a topic, e.g it
 * can pause the message flow when it needs more time to process the actual messages and then resume
 * to continue message processing.
 *
 * To achieve that you can use {@link io.vertx.kafka.client.consumer.KafkaConsumer#pause} and
 * {@link io.vertx.kafka.client.consumer.KafkaConsumer#resume}
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleConsumerFlowControl}
 * ----
 *
 * == Closing a consumer
 *
 * Call close to close the consumer. Closing the consumer closes any open connections and releases all consumer resources.
 *
 * The close is actually asynchronous and might not complete until some time after the call has returned. If you want to be notified
 * when the actual close has completed then you can pass in a handler.
 *
 * This handler will then be called when the close has fully completed.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleConsumerClose(io.vertx.kafka.client.consumer.KafkaConsumer)}
 * ----
 *
 * == Sending messages to a topic
 *
 * You can use  {@link io.vertx.kafka.client.producer.KafkaProducer#write} to send messages (records) to a topic.
 *
 * The simplest way to send a message is to specify only the destination topic and the related value, omitting its key
 * or partition, in this case the messages are sent in a round robin fashion across all the partitions of the topic.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleProducerWrite}
 * ----
 *
 * You can receive message sent metadata like its topic, its destination partition and its assigned offset.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleProducerWriteWithAck}
 * ----
 *
 * When you need to assign a partition to a message, you can specify its partition identifier
 * or its key
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleProducerWriteWithSpecificPartition}
 * ----
 *
 * Since the producers identifies the destination using key hashing, you can use that to guarantee that all
 * messages with the same key are sent to the same partition and retain the order.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleProducerWriteWithSpecificKey}
 * ----
 *
 * NOTE: the shared producer is created on the first `createShared` call and its configuration is defined at this moment,
 * shared producer usage must use the same configuration.
 *
 * == Sharing a producer
 *
 * Sometimes you want to share the same producer from within several verticles or contexts.
 *
 * Calling {@link io.vertx.kafka.client.producer.KafkaProducer#createShared(io.vertx.core.Vertx, java.lang.String, java.util.Map)}
 * returns a producer that can be shared safely.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleSharedProducer}
 * ----
 *
 * The same resources (thread, connection) will be shared between the producer returned by this method.
 *
 * When you are done with the producer, just close it, when all shared producers are closed, the resources will
 * be released for you.
 *
 * == Closing a producer
 *
 * Call close to close the producer. Closing the producer closes any open connections and releases all producer resources.
 *
 * The close is actually asynchronous and might not complete until some time after the call has returned. If you want to be notified
 * when the actual close has completed then you can pass in a handler.
 *
 * This handler will then be called when the close has fully completed.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleProducerClose(io.vertx.kafka.client.producer.KafkaProducer)}
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
 * == Handling errors
 *
 * Errors handling (e.g timeout) between a Kafka client (consumer or producer) and the Kafka cluster is done using
 * {@link io.vertx.kafka.client.consumer.KafkaConsumer#exceptionHandler} or
 * {@link io.vertx.kafka.client.producer.KafkaProducer#exceptionHandler}
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleErrorHandling}
 * ----
 *
 * == Automatic clean-up in verticles
 *
 * If you’re creating consumers and producer from inside verticles, those consumers and producers will be automatically
 * closed when the verticle is undeployed.
 *
 * == Using Vert.x serializers/deserizaliers
 *
 * Vert.x Kafka client comes out of the box with serializers and deserializers for buffers, json object
 * and json array.
 *
 * In a consumer you can use buffers
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleUsingVertxDeserializers()}
 * ----
 *
 * Or in a producer
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleUsingVertxSerializers()}
 * ----
 *
 * ifdef::java,groovy,kotlin[]
 * You can also specify the serizalizers/deserializers at creation time:
 *
 * In a consumer
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleUsingVertxDeserializers2(io.vertx.core.Vertx)}
 * ----
 *
 * Or in a producer
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxKafkaClientExamples#exampleUsingVertxSerializers2(io.vertx.core.Vertx)}
 * ----
 *
 * endif::[]
 *
 * ifdef::java[]
 * == RxJava API
 *
 * The Kafka client provides an Rxified version of the original API.
 *
 * [source,$lang]
 * ----
 * {@link examples.RxExamples#consumer(io.vertx.rxjava.kafka.client.consumer.KafkaConsumer)}
 * ----
 * endif::[]
 *
 * ifdef::java,groovy,kotlin[]
 * == Stream implementation and native Kafka objects
 *
 * When you want to operate on native Kafka records you can use a stream oriented
 * implementation which handles native Kafka objects.
 *
 * The {@link io.vertx.kafka.client.consumer.KafkaReadStream} shall be used for reading topic partitions, it is
 * a read stream of {@link org.apache.kafka.clients.consumer.ConsumerRecord} objects.
 *
 * The {@link io.vertx.kafka.client.producer.KafkaWriteStream} shall be used for writing to topics, it is a write
 * stream of {@link org.apache.kafka.clients.producer.ProducerRecord}.
 *
 * The API exposed by these interfaces is mostly the same than the polyglot version.
 * endif::[]
 */
@Document(fileName = "index.adoc")
@ModuleGen(name = "vertx-kafka-client", groupPackage = "io.vertx")
package io.vertx.kafka.client;

import io.vertx.codegen.annotations.ModuleGen;
import io.vertx.docgen.Document;

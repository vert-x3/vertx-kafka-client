# Vert.x Kafka Client

[![Build Status](https://github.com/vert-x3/vertx-kafka-client/workflows/CI/badge.svg?branch=master)](https://github.com/vert-x3/vertx-kafka-client/actions?query=workflow%3ACI)

This component provides a Kafka client for reading and sending messages from/to an [Apache Kafka](https://kafka.apache.org/) cluster.
From the consumer point of view, its API provides a bunch of methods for subscribing to a topic partition receiving
messages asynchronously or reading them as a stream (even with the possibility to pause the stream itself).
As producer, its API provides methods for sending message to a topic partition like writing on a stream.

See the online docs for more details:
- [Java](https://vertx.io/docs/vertx-kafka-client/java)
- [JavaScript](https://vertx.io/docs/vertx-kafka-client/js)
- [Ruby](https://vertx.io/docs/vertx-kafka-client/ruby)
- [Groovy](https://vertx.io/docs/vertx-kafka-client/groovy)
- [Kotlin](https://vertx.io/docs/vertx-kafka-client/kotlin)

Important aspects of Topic Management, such as creating a topic, deleting a topic, changing configuration of a topic, are also supported.
See the online docs for more details:
- [Java](https://vertx.io/docs/vertx-kafka-client/java/#_vert_x_kafka_adminutils)
- [JavaScript](https://vertx.io/docs/vertx-kafka-client/js/#_vert_x_kafka_adminutils)
- [Ruby](https://vertx.io/docs/vertx-kafka-client/ruby/#_vert_x_kafka_adminutils)
- [Groovy](https://vertx.io/docs/vertx-kafka-client/groovy/#_vert_x_kafka_adminutils)
- [Kotlin](https://vertx.io/docs/vertx-kafka-client/kotlin/#_vert_x_kafka_adminutils)

**Note: This module has Tech Preview status, this means the API can change between versions.**

# Vert.x Kafka Client

[![Build Status (5.x)](https://github.com/vert-x3/vertx-kafka-client/actions/workflows/ci-5.x.yml/badge.svg)](https://github.com/vert-x3/vertx-kafka-client/actions/workflows/ci-5.x.yml)
[![Build Status (4.x)](https://github.com/vert-x3/vertx-kafka-client/actions/workflows/ci-4.x.yml/badge.svg)](https://github.com/vert-x3/vertx-kafka-client/actions/workflows/ci-4.x.yml)

This component provides a Kafka client for reading and sending messages from/to an [Apache Kafka](https://kafka.apache.org/) cluster.
From the consumer point of view, its API provides a bunch of methods for subscribing to a topic partition receiving
messages asynchronously or reading them as a stream (even with the possibility to pause the stream itself).
As producer, its API provides methods for sending message to a topic partition like writing on a stream.

See the online docs for more details:
- [Java](https://vertx.io/docs/vertx-kafka-client/java)

Important aspects of Topic Management, such as creating a topic, deleting a topic, changing configuration of a topic, are also supported.
See the online docs for more details:
- [Topic Management](https://vertx.io/docs/vertx-kafka-client/java/#_vert_x_kafka_admin_client)

**Note: This module has Tech Preview status, this means the API can change between versions.**

## Running tests

The full test suite starts Kafka brokers using Strimzi Testcontainers, so Docker must be installed and running before executing:

```bash
mvn clean verify
```

Some mock and unit tests do not start Kafka containers and can be run without Docker, for example:

```bash
mvn -Dtest=ProducerMockTest,ConsumerMockTest,KafkaReadStreamMockTest,NoClusterTest,RetryHelperTest,KafkaHeaderTest,TopicPartitionTest test
```

This runs only the specified tests, not the full test suite.

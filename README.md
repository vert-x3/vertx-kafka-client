[![Build Status](https://vertx.ci.cloudbees.com/buildStatus/icon?job=vert.x3-kafka-client)](https://vertx.ci.cloudbees.com/view/vert.x-3/job/vert.x3-kafka-client/)

# Vert.x Kafka Client

This component provides a Kafka client for reading and sending messages from/to an [Apache Kafka](https://kafka.apache.org/) cluster.
From the consumer point of view, its API provides a bunch of methods for subscribing to a topic partition receiving
messages asynchronously or reading them as a stream (even with the possibility to pause the stream itself).
As producer, its API provides methods for sending message to a topic partition like writing on a stream.

See the in-source docs for more details:
- [Java](src/main/asciidoc/java/index.adoc).
- [Groovy](src/main/asciidoc/groovy/index.adoc).

**Note: This module has Tech Preview status, this means the API can change between versions.**

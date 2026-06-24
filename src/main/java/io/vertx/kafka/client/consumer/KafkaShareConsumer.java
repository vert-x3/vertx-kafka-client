package io.vertx.kafka.client.consumer;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.ReadStream;
import io.vertx.kafka.client.common.KafkaClientOptions;
import io.vertx.kafka.client.consumer.impl.KafkaShareConsumerImpl;
import io.vertx.kafka.client.consumer.impl.KafkaShareReadStreamImpl;
import io.vertx.kafka.client.serialization.VertxSerdes;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.*;

import static io.vertx.codegen.annotations.GenIgnore.PERMITTED_TYPE;

@VertxGen
public interface KafkaShareConsumer<K, V> extends ReadStream<KafkaShareConsumerRecord<K, V>> {

  /**
   * Create a new {@code KafkaShareConsumer} wrapping a native Kafka {@link ShareConsumer}.
   *
   * @param vertx         the Vert.x instance
   * @param shareConsumer the native Kafka share consumer to wrap
   * @return a new {@code KafkaShareConsumer}
   */
  @GenIgnore(PERMITTED_TYPE)
  static <K, V> KafkaShareConsumer<K, V> create(Vertx vertx, ShareConsumer<K, V> shareConsumer) {
    KafkaShareReadStreamImpl<K, V> stream = new KafkaShareReadStreamImpl<>(vertx, shareConsumer, new KafkaClientOptions());
    return new KafkaShareConsumerImpl<>(stream).registerCloseHook();
  }

  /**
   * Create a new {@code KafkaShareConsumer} from a {@link Map} configuration.
   *
   * @param vertx  the Vert.x instance
   * @param config Kafka consumer configuration
   * @return a new {@code KafkaShareConsumer}
   */
  static <K, V> KafkaShareConsumer<K, V> create(Vertx vertx, Map<String, String> config) {
    KafkaShareReadStreamImpl<K, V> stream = new KafkaShareReadStreamImpl<>(vertx, new org.apache.kafka.clients.consumer.KafkaShareConsumer<>(new HashMap<>(config)), new KafkaClientOptions());
    return new KafkaShareConsumerImpl<>(stream).registerCloseHook();
  }

  /**
   * Create a new {@code KafkaShareConsumer} from a {@link Map} configuration with explicit deserializer types.
   *
   * @param vertx     the Vert.x instance
   * @param config    Kafka consumer configuration
   * @param keyType   class type for the key deserialization
   * @param valueType class type for the value deserialization
   * @return a new {@code KafkaShareConsumer}
   */
  static <K, V> KafkaShareConsumer<K, V> create(Vertx vertx, Map<String, String> config,
                                                Class<K> keyType, Class<V> valueType) {
    Deserializer<K> keyDeserializer = VertxSerdes.serdeFrom(keyType).deserializer();
    Deserializer<V> valueDeserializer = VertxSerdes.serdeFrom(valueType).deserializer();
    return create(vertx, config, keyDeserializer, valueDeserializer);
  }

  /**
   * Create a new {@code KafkaShareConsumer} from a {@link Map} configuration with explicit deserializers.
   *
   * @param vertx             the Vert.x instance
   * @param config            Kafka consumer configuration
   * @param keyDeserializer   key deserializer
   * @param valueDeserializer value deserializer
   * @return a new {@code KafkaShareConsumer}
   */
  @GenIgnore
  static <K, V> KafkaShareConsumer<K, V> create(Vertx vertx, Map<String, String> config,
                                                Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
    KafkaShareReadStreamImpl<K, V> stream = new KafkaShareReadStreamImpl<>(vertx,
      new org.apache.kafka.clients.consumer.KafkaShareConsumer<>(new HashMap<>(config), keyDeserializer, valueDeserializer), new KafkaClientOptions());
    return new KafkaShareConsumerImpl<>(stream).registerCloseHook();
  }

  /**
   * Create a new {@code KafkaShareConsumer} from {@link KafkaClientOptions}.
   *
   * @param vertx   the Vert.x instance
   * @param options Kafka client options
   * @return a new {@code KafkaShareConsumer}
   */
  static <K, V> KafkaShareConsumer<K, V> create(Vertx vertx, KafkaClientOptions options) {
    Map<String, Object> config = new HashMap<>();
    if (options.getConfig() != null) config.putAll(options.getConfig());
    KafkaShareReadStreamImpl<K, V> stream = new KafkaShareReadStreamImpl<>(vertx, new org.apache.kafka.clients.consumer.KafkaShareConsumer<>(config), options);
    return new KafkaShareConsumerImpl<>(stream).registerCloseHook();
  }

  /**
   * Create a new {@code KafkaShareConsumer} from {@link KafkaClientOptions} with explicit deserializer types.
   *
   * @param vertx     the Vert.x instance
   * @param options   Kafka client options
   * @param keyType   class type for the key deserialization
   * @param valueType class type for the value deserialization
   * @return a new {@code KafkaShareConsumer}
   */
  static <K, V> KafkaShareConsumer<K, V> create(Vertx vertx, KafkaClientOptions options,
                                                Class<K> keyType, Class<V> valueType) {
    Deserializer<K> keyDeserializer = VertxSerdes.serdeFrom(keyType).deserializer();
    Deserializer<V> valueDeserializer = VertxSerdes.serdeFrom(valueType).deserializer();
    return create(vertx, options, keyDeserializer, valueDeserializer);
  }

  /**
   * Create a new {@code KafkaShareConsumer} from {@link KafkaClientOptions} with explicit deserializers.
   *
   * @param vertx             the Vert.x instance
   * @param options           Kafka client options
   * @param keyDeserializer   key deserializer
   * @param valueDeserializer value deserializer
   * @return a new {@code KafkaShareConsumer}
   */
  @GenIgnore
  static <K, V> KafkaShareConsumer<K, V> create(Vertx vertx, KafkaClientOptions options,
                                                Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
    Map<String, Object> config = new HashMap<>();
    if (options.getConfig() != null) config.putAll(options.getConfig());
    KafkaShareReadStreamImpl<K, V> stream = new KafkaShareReadStreamImpl<>(vertx,
      new org.apache.kafka.clients.consumer.KafkaShareConsumer<>(config, keyDeserializer, valueDeserializer), options);
    return new KafkaShareConsumerImpl<>(stream).registerCloseHook();
  }

  /**
   * Create a new {@code KafkaShareConsumer} from a {@link Properties} configuration.
   *
   * @param vertx  the Vert.x instance
   * @param config Kafka consumer configuration
   * @return a new {@code KafkaShareConsumer}
   */
  @GenIgnore
  static <K, V> KafkaShareConsumer<K, V> create(Vertx vertx, Properties config) {
    KafkaShareReadStreamImpl<K, V> stream = new KafkaShareReadStreamImpl<>(vertx, new org.apache.kafka.clients.consumer.KafkaShareConsumer<>(config), new KafkaClientOptions());
    return new KafkaShareConsumerImpl<>(stream).registerCloseHook();
  }

  /**
   * Create a new {@code KafkaShareConsumer} from a {@link Properties} configuration with explicit deserializer types.
   *
   * @param vertx     the Vert.x instance
   * @param config    Kafka consumer configuration
   * @param keyType   class type for the key deserialization
   * @param valueType class type for the value deserialization
   * @return a new {@code KafkaShareConsumer}
   */
  @GenIgnore
  static <K, V> KafkaShareConsumer<K, V> create(Vertx vertx, Properties config,
                                                Class<K> keyType, Class<V> valueType) {
    Deserializer<K> keyDeserializer = VertxSerdes.serdeFrom(keyType).deserializer();
    Deserializer<V> valueDeserializer = VertxSerdes.serdeFrom(valueType).deserializer();
    return create(vertx, config, keyDeserializer, valueDeserializer);
  }

  /**
   * Create a new {@code KafkaShareConsumer} from a {@link Properties} configuration with explicit deserializers.
   *
   * @param vertx             the Vert.x instance
   * @param config            Kafka consumer configuration
   * @param keyDeserializer   key deserializer
   * @param valueDeserializer value deserializer
   * @return a new {@code KafkaShareConsumer}
   */
  @GenIgnore
  static <K, V> KafkaShareConsumer<K, V> create(Vertx vertx, Properties config,
                                                Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
    KafkaShareReadStreamImpl<K, V> stream = new KafkaShareReadStreamImpl<>(vertx,
      new org.apache.kafka.clients.consumer.KafkaShareConsumer<>(config, keyDeserializer, valueDeserializer), new KafkaClientOptions());
    return new KafkaShareConsumerImpl<>(stream).registerCloseHook();
  }

  @Fluent
  @Override
  KafkaShareConsumer<K, V> exceptionHandler(Handler<Throwable> handler);

  @Fluent
  @Override
  KafkaShareConsumer<K, V> handler(Handler<KafkaShareConsumerRecord<K, V>> handler);

  /**
   * Set a handler to receive records in batches rather than one at a time.
   * <p>
   * When set, this takes precedence over the per-record {@link #handler}. The entire
   * batch returned by a single poll is delivered as a {@link KafkaShareConsumerRecords}
   * to this handler. Useful when processing efficiency matters more than per-record flow control.
   *
   * @param handler the batch handler
   * @return a reference to this, so the API can be used fluently
   */
  @Fluent
  KafkaShareConsumer<K, V> batchHandler(Handler<KafkaShareConsumerRecords<K, V>> handler);

  /**
   * Close the consumer, releasing all resources.
   *
   * @return a future completed when the consumer is closed
   */
  Future<Void> close();

  /**
   * Subscribe to a single topic. The consumer will start receiving records via the
   * {@link #handler} once subscription is established.
   *
   * @param topic the topic to subscribe to
   * @return a future completed when the subscription is established
   */
  Future<Void> subscribe(String topic);

  /**
   * Subscribe to a set of topics. The consumer will start receiving records via the
   * {@link #handler} once subscription is established.
   *
   * @param topics the topics to subscribe to
   * @return a future completed when the subscription is established
   */
  Future<Void> subscribe(Set<String> topics);

  /**
   * @return a future completed with the set of topics this consumer is currently subscribed to
   */
  Future<Set<String>> subscription();

  /**
   * Unsubscribe from all topics.
   *
   * @return a future completed when the consumer has unsubscribed
   */
  Future<Void> unsubscribe();

  /**
   * Manually poll a batch of records from the broker, waiting up to {@code timeout} for records
   * to become available.
   * <p>
   * Use this method when you want explicit control over when polling occurs. When using
   * the streaming API ({@link #handler}), polling is managed internally and this method
   * should not be called concurrently.
   *
   * @param timeout the maximum time to wait for records
   * @return a future completed with the polled records
   */
  @GenIgnore(PERMITTED_TYPE)
  Future<KafkaShareConsumerRecords<K, V>> poll(Duration timeout);

  /**
   * Commit the acknowledgements for the records returned by the last poll asynchronously.
   * <p>
   * This is a fire-and-forget operation. To be notified of the commit result,
   * register a callback with {@link #setAcknowledgementCommitCallback}.
   *
   * @return a reference to this, so the API can be used fluently
   */
  @Fluent
  KafkaShareConsumer<K, V> commitAsync();

  /**
   * Set a callback to be notified of the result of {@link #commitAsync} operations.
   * <p>
   * The callback receives a map of per-partition results and an overall exception
   * (which is {@code null} on success). The callback is always invoked on the Vert.x
   * event loop.
   *
   * @param callback the callback to invoke after each async commit
   * @return a reference to this, so the API can be used fluently
   */
  @GenIgnore
  KafkaShareConsumer<K, V> setAcknowledgementCommitCallback(AcknowledgementCommitCallback callback);

  /**
   * Acknowledge delivery of a record returned by the last {@link #poll} call.
   * <p>
   * The {@link AcknowledgeType} indicates how the record was processed:
   * <ul>
   *   <li>{@link AcknowledgeType#ACCEPT} — record was processed successfully,
   *       it will not be redelivered</li>
   *   <li>{@link AcknowledgeType#RELEASE} — record was not processed, it will
   *       be made available for redelivery to another consumer</li>
   *   <li>{@link AcknowledgeType#REJECT} — record is unprocessable, it will
   *       not be redelivered to any consumer</li>
   *   <li>{@link AcknowledgeType#RENEW} — record is still being processed,
   *       the acquisition lock will be extended and the record will be
   *       returned again on the next {@link #poll} call</li>
   * </ul>
   * <p>
   * This method only updates a local in-memory state. The acknowledgement
   * is sent to the broker on the next {@link #commitSync()},
   * {@link #commitAsync()} or {@link #poll} call.
   * <p>
   * This method can only be used if the consumer is configured with
   * <b>explicit acknowledgement</b> ({@code share.acknowledgement.mode=explicit}).
   *
   * @param record the record to acknowledge
   * @param type   the acknowledgement type indicating how the record was processed
   * @return a future completed when the local acknowledgement state is updated
   */
  Future<Void> acknowledge(KafkaShareConsumerRecord<K, V> record, AcknowledgeType type);

  /**
   * Commit the acknowledgements for the records returned by the last {@link #poll} call.
   * <p>
   * If the consumer is using <b>explicit acknowledgement</b>, only the records acknowledged
   * using {@link #acknowledge} will be committed. If the consumer is using
   * <b>implicit acknowledgement</b>, all records returned by the last poll are committed.
   * <p>
   * This is a synchronous commit that blocks until either the commit succeeds or fails.
   * The timeout is controlled by the {@code default.api.timeout.ms} configuration property.
   * <p>
   * If a single partition fails, the future fails with the original exception.
   * If multiple partitions fail, the future fails with a summary exception listing
   * each failed partition and its error on a separate line.
   * <p>
   * For full per-partition result control use {@link #commitSync(Duration)}.
   *
   * @return a future completed when all acknowledgements are committed successfully,
   * or failed if one or more partitions could not be committed
   */
  Future<Void> commitSync();

  /**
   * Commit the acknowledgements for the records returned by the last {@link #poll} call,
   * waiting up to the specified timeout for the operation to complete.
   * <p>
   * If the consumer is using <b>explicit acknowledgement</b>, only the records acknowledged
   * using {@link #acknowledge} will be committed. If the consumer is using
   * <b>implicit acknowledgement</b>, all records returned by the last poll are committed.
   * <p>
   * Unlike {@link #commitSync()}, this method returns the full per-partition result map,
   * allowing the caller to inspect which partitions succeeded and which failed independently.
   * A partition that failed will have an {@link java.util.Optional} containing the exception,
   * a partition that succeeded will have an empty {@link java.util.Optional}.
   * <p>
   * Example usage:
   * <pre>
   *   consumer.commitSync(Duration.ofSeconds(5)).onComplete(ar -> {
   *     if (ar.succeeded()) {
   *       ar.result().forEach((partition, error) -> {
   *         if (error.isPresent()) {
   *           log.error("Partition {} failed: {}", partition, error.get().getMessage());
   *         }
   *       });
   *     }
   *   });
   * </pre>
   *
   * @param timeout the maximum time to wait for the commit to complete,
   *                if {@code null} the broker default ({@code default.api.timeout.ms})
   *                will be used
   * @return a future completed with a map of per-partition results
   */
  @GenIgnore
  Future<Map<TopicIdPartition, Optional<KafkaException>>> commitSync(Duration timeout);

  /**
   * Set the timeout used for internal polling in streaming mode.
   * <p>
   * This controls how long the internal poll loop blocks waiting for records from
   * the broker on each iteration. A shorter timeout makes the consumer more responsive
   * to close and pause requests; a longer timeout reduces CPU overhead when topics
   * are sparse. Defaults to 1 second.
   *
   * @param timeout the poll timeout
   * @return a reference to this, so the API can be used fluently
   */
  @Fluent
  @GenIgnore(PERMITTED_TYPE)
  KafkaShareConsumer<K, V> pollTimeout(Duration timeout);

  /**
   * Return the underlying native Kafka {@link ShareConsumer}.
   * <p>
   * Use with care — direct calls on the native consumer bypass the Vert.x threading
   * model and may conflict with the internal poll loop.
   *
   * @return the native Kafka share consumer
   */
  @GenIgnore(PERMITTED_TYPE)
  ShareConsumer<K, V> unwrap();
}

require 'vertx-kafka-client/kafka_consumer_record'
require 'vertx/vertx'
require 'vertx/read_stream'
require 'vertx/util/utils.rb'
# Generated from io.vertx.kafka.client.consumer.KafkaConsumer
module VertxKafkaClient
  #  Vert.x Kafka consumer.
  #  <p>
  #  You receive Kafka records by providing a {::VertxKafkaClient::KafkaConsumer#handler}. As messages arrive the handler
  #  will be called with the records.
  #  <p>
  #  The {::VertxKafkaClient::KafkaConsumer#pause} and {::VertxKafkaClient::KafkaConsumer#resume} provides global control over reading the records from the consumer.
  #  <p>
  #  The {::VertxKafkaClient::KafkaConsumer#pause} and {::VertxKafkaClient::KafkaConsumer#resume} provides finer grained control over reading records
  #  for specific Topic/Partition, these are Kafka's specific operations.
  class KafkaConsumer
    include ::Vertx::ReadStream
    # @private
    # @param j_del [::VertxKafkaClient::KafkaConsumer] the java delegate
    def initialize(j_del, j_arg_K=nil, j_arg_V=nil)
      @j_del = j_del
      @j_arg_K = j_arg_K != nil ? j_arg_K : ::Vertx::Util::unknown_type
      @j_arg_V = j_arg_V != nil ? j_arg_V : ::Vertx::Util::unknown_type
    end
    # @private
    # @return [::VertxKafkaClient::KafkaConsumer] the underlying java delegate
    def j_del
      @j_del
    end
    #  Create a new KafkaConsumer instance
    # @param [::Vertx::Vertx] vertx Vert.x instance to use
    # @param [Hash{String => String}] config Kafka consumer configuration
    # @param [Nil] keyType class type for the key deserialization
    # @param [Nil] valueType class type for the value deserialization
    # @return [::VertxKafkaClient::KafkaConsumer] an instance of the KafkaConsumer
    def self.create(vertx=nil,config=nil,keyType=nil,valueType=nil)
      if vertx.class.method_defined?(:j_del) && config.class == Hash && !block_given? && keyType == nil && valueType == nil
        return ::Vertx::Util::Utils.safe_create(Java::IoVertxKafkaClientConsumer::KafkaConsumer.java_method(:create, [Java::IoVertxCore::Vertx.java_class,Java::JavaUtil::Map.java_class]).call(vertx.j_del,Hash[config.map { |k,v| [k,v] }]),::VertxKafkaClient::KafkaConsumer, nil, nil)
      elsif vertx.class.method_defined?(:j_del) && config.class == Hash && keyType.class == Class && valueType.class == Class && !block_given?
        return ::Vertx::Util::Utils.safe_create(Java::IoVertxKafkaClientConsumer::KafkaConsumer.java_method(:create, [Java::IoVertxCore::Vertx.java_class,Java::JavaUtil::Map.java_class,Java::JavaLang::Class.java_class,Java::JavaLang::Class.java_class]).call(vertx.j_del,Hash[config.map { |k,v| [k,v] }],::Vertx::Util::Utils.j_class_of(keyType),::Vertx::Util::Utils.j_class_of(valueType)),::VertxKafkaClient::KafkaConsumer, ::Vertx::Util::Utils.v_type_of(keyType), ::Vertx::Util::Utils.v_type_of(valueType))
      end
      raise ArgumentError, "Invalid arguments when calling create(#{vertx},#{config},#{keyType},#{valueType})"
    end
    # @yield 
    # @return [self]
    def exception_handler
      if block_given?
        @j_del.java_method(:exceptionHandler, [Java::IoVertxCore::Handler.java_class]).call((Proc.new { |event| yield(::Vertx::Util::Utils.from_throwable(event)) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling exception_handler()"
    end
    # @yield 
    # @return [self]
    def handler
      if block_given?
        @j_del.java_method(:handler, [Java::IoVertxCore::Handler.java_class]).call((Proc.new { |event| yield(::Vertx::Util::Utils.safe_create(event,::VertxKafkaClient::KafkaConsumerRecord, nil, nil)) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling handler()"
    end
    #  Suspend fetching from the requested partitions.
    # @overload pause()
    # @overload pause(topicPartition)
    #   @param [Hash] topicPartition topic partition from which suspend fetching
    # @overload pause(topicPartitions)
    #   @param [Set<Hash>] topicPartitions topic partition from which suspend fetching
    # @overload pause(topicPartition,completionHandler)
    #   @param [Hash] topicPartition topic partition from which suspend fetching
    #   @yield handler called on operation completed
    # @overload pause(topicPartitions,completionHandler)
    #   @param [Set<Hash>] topicPartitions topic partition from which suspend fetching
    #   @yield handler called on operation completed
    # @return [self]
    def pause(param_1=nil)
      if !block_given? && param_1 == nil
        @j_del.java_method(:pause, []).call()
        return self
      elsif param_1.class == Hash && !block_given?
        @j_del.java_method(:pause, [Java::IoVertxKafkaClientCommon::TopicPartition.java_class]).call(Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(param_1)))
        return self
      elsif param_1.class == Set && !block_given?
        @j_del.java_method(:pause, [Java::JavaUtil::Set.java_class]).call(Java::JavaUtil::LinkedHashSet.new(param_1.map { |element| Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(element)) }))
        return self
      elsif param_1.class == Hash && block_given?
        @j_del.java_method(:pause, [Java::IoVertxKafkaClientCommon::TopicPartition.java_class,Java::IoVertxCore::Handler.java_class]).call(Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(param_1)),(Proc.new { |ar| yield(ar.failed ? ar.cause : nil) }))
        return self
      elsif param_1.class == Set && block_given?
        @j_del.java_method(:pause, [Java::JavaUtil::Set.java_class,Java::IoVertxCore::Handler.java_class]).call(Java::JavaUtil::LinkedHashSet.new(param_1.map { |element| Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(element)) }),(Proc.new { |ar| yield(ar.failed ? ar.cause : nil) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling pause(#{param_1})"
    end
    #  Resume specified partitions which have been paused with pause.
    # @overload resume()
    # @overload resume(topicPartition)
    #   @param [Hash] topicPartition topic partition from which resume fetching
    # @overload resume(topicPartitions)
    #   @param [Set<Hash>] topicPartitions topic partition from which resume fetching
    # @overload resume(topicPartition,completionHandler)
    #   @param [Hash] topicPartition topic partition from which resume fetching
    #   @yield handler called on operation completed
    # @overload resume(topicPartitions,completionHandler)
    #   @param [Set<Hash>] topicPartitions topic partition from which resume fetching
    #   @yield handler called on operation completed
    # @return [self]
    def resume(param_1=nil)
      if !block_given? && param_1 == nil
        @j_del.java_method(:resume, []).call()
        return self
      elsif param_1.class == Hash && !block_given?
        @j_del.java_method(:resume, [Java::IoVertxKafkaClientCommon::TopicPartition.java_class]).call(Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(param_1)))
        return self
      elsif param_1.class == Set && !block_given?
        @j_del.java_method(:resume, [Java::JavaUtil::Set.java_class]).call(Java::JavaUtil::LinkedHashSet.new(param_1.map { |element| Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(element)) }))
        return self
      elsif param_1.class == Hash && block_given?
        @j_del.java_method(:resume, [Java::IoVertxKafkaClientCommon::TopicPartition.java_class,Java::IoVertxCore::Handler.java_class]).call(Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(param_1)),(Proc.new { |ar| yield(ar.failed ? ar.cause : nil) }))
        return self
      elsif param_1.class == Set && block_given?
        @j_del.java_method(:resume, [Java::JavaUtil::Set.java_class,Java::IoVertxCore::Handler.java_class]).call(Java::JavaUtil::LinkedHashSet.new(param_1.map { |element| Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(element)) }),(Proc.new { |ar| yield(ar.failed ? ar.cause : nil) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling resume(#{param_1})"
    end
    # @yield 
    # @return [self]
    def end_handler
      if block_given?
        @j_del.java_method(:endHandler, [Java::IoVertxCore::Handler.java_class]).call(Proc.new { yield })
        return self
      end
      raise ArgumentError, "Invalid arguments when calling end_handler()"
    end
    #  Subscribe to the given list of topics to get dynamically assigned partitions.
    # @overload subscribe(topic)
    #   @param [String] topic topic to subscribe to
    # @overload subscribe(topics)
    #   @param [Set<String>] topics topics to subscribe to
    # @overload subscribe(topic,completionHandler)
    #   @param [String] topic topic to subscribe to
    #   @yield handler called on operation completed
    # @overload subscribe(topics,completionHandler)
    #   @param [Set<String>] topics topics to subscribe to
    #   @yield handler called on operation completed
    # @return [self]
    def subscribe(param_1=nil)
      if param_1.class == String && !block_given?
        @j_del.java_method(:subscribe, [Java::java.lang.String.java_class]).call(param_1)
        return self
      elsif param_1.class == Set && !block_given?
        @j_del.java_method(:subscribe, [Java::JavaUtil::Set.java_class]).call(Java::JavaUtil::LinkedHashSet.new(param_1.map { |element| element }))
        return self
      elsif param_1.class == String && block_given?
        @j_del.java_method(:subscribe, [Java::java.lang.String.java_class,Java::IoVertxCore::Handler.java_class]).call(param_1,(Proc.new { |ar| yield(ar.failed ? ar.cause : nil) }))
        return self
      elsif param_1.class == Set && block_given?
        @j_del.java_method(:subscribe, [Java::JavaUtil::Set.java_class,Java::IoVertxCore::Handler.java_class]).call(Java::JavaUtil::LinkedHashSet.new(param_1.map { |element| element }),(Proc.new { |ar| yield(ar.failed ? ar.cause : nil) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling subscribe(#{param_1})"
    end
    #  Manually assign a list of partition to this consumer.
    # @overload assign(topicPartition)
    #   @param [Hash] topicPartition partition which want assigned
    # @overload assign(topicPartitions)
    #   @param [Set<Hash>] topicPartitions partitions which want assigned
    # @overload assign(topicPartition,completionHandler)
    #   @param [Hash] topicPartition partition which want assigned
    #   @yield handler called on operation completed
    # @overload assign(topicPartitions,completionHandler)
    #   @param [Set<Hash>] topicPartitions partitions which want assigned
    #   @yield handler called on operation completed
    # @return [self]
    def assign(param_1=nil)
      if param_1.class == Hash && !block_given?
        @j_del.java_method(:assign, [Java::IoVertxKafkaClientCommon::TopicPartition.java_class]).call(Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(param_1)))
        return self
      elsif param_1.class == Set && !block_given?
        @j_del.java_method(:assign, [Java::JavaUtil::Set.java_class]).call(Java::JavaUtil::LinkedHashSet.new(param_1.map { |element| Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(element)) }))
        return self
      elsif param_1.class == Hash && block_given?
        @j_del.java_method(:assign, [Java::IoVertxKafkaClientCommon::TopicPartition.java_class,Java::IoVertxCore::Handler.java_class]).call(Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(param_1)),(Proc.new { |ar| yield(ar.failed ? ar.cause : nil) }))
        return self
      elsif param_1.class == Set && block_given?
        @j_del.java_method(:assign, [Java::JavaUtil::Set.java_class,Java::IoVertxCore::Handler.java_class]).call(Java::JavaUtil::LinkedHashSet.new(param_1.map { |element| Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(element)) }),(Proc.new { |ar| yield(ar.failed ? ar.cause : nil) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling assign(#{param_1})"
    end
    #  Get the set of partitions currently assigned to this consumer.
    # @yield handler called on operation completed
    # @return [self]
    def assignment
      if block_given?
        @j_del.java_method(:assignment, [Java::IoVertxCore::Handler.java_class]).call((Proc.new { |ar| yield(ar.failed ? ar.cause : nil, ar.succeeded ? ::Vertx::Util::Utils.to_set(ar.result).map! { |elt| elt != nil ? JSON.parse(elt.toJson.encode) : nil } : nil) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling assignment()"
    end
    #  Unsubscribe from topics currently subscribed with subscribe.
    # @yield handler called on operation completed
    # @return [self]
    def unsubscribe
      if !block_given?
        @j_del.java_method(:unsubscribe, []).call()
        return self
      elsif block_given?
        @j_del.java_method(:unsubscribe, [Java::IoVertxCore::Handler.java_class]).call((Proc.new { |ar| yield(ar.failed ? ar.cause : nil) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling unsubscribe()"
    end
    #  Get the current subscription.
    # @yield handler called on operation completed
    # @return [self]
    def subscription
      if block_given?
        @j_del.java_method(:subscription, [Java::IoVertxCore::Handler.java_class]).call((Proc.new { |ar| yield(ar.failed ? ar.cause : nil, ar.succeeded ? ::Vertx::Util::Utils.to_set(ar.result).map! { |elt| elt } : nil) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling subscription()"
    end
    #  Get the set of partitions that were previously paused by a call to pause(Set).
    # @yield handler called on operation completed
    # @return [void]
    def paused
      if block_given?
        return @j_del.java_method(:paused, [Java::IoVertxCore::Handler.java_class]).call((Proc.new { |ar| yield(ar.failed ? ar.cause : nil, ar.succeeded ? ::Vertx::Util::Utils.to_set(ar.result).map! { |elt| elt != nil ? JSON.parse(elt.toJson.encode) : nil } : nil) }))
      end
      raise ArgumentError, "Invalid arguments when calling paused()"
    end
    #  Set the handler called when topic partitions are revoked to the consumer
    # @yield handler called on revoked topic partitions
    # @return [self]
    def partitions_revoked_handler
      if block_given?
        @j_del.java_method(:partitionsRevokedHandler, [Java::IoVertxCore::Handler.java_class]).call((Proc.new { |event| yield(::Vertx::Util::Utils.to_set(event).map! { |elt| elt != nil ? JSON.parse(elt.toJson.encode) : nil }) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling partitions_revoked_handler()"
    end
    #  Set the handler called when topic partitions are assigned to the consumer
    # @yield handler called on assigned topic partitions
    # @return [self]
    def partitions_assigned_handler
      if block_given?
        @j_del.java_method(:partitionsAssignedHandler, [Java::IoVertxCore::Handler.java_class]).call((Proc.new { |event| yield(::Vertx::Util::Utils.to_set(event).map! { |elt| elt != nil ? JSON.parse(elt.toJson.encode) : nil }) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling partitions_assigned_handler()"
    end
    #  Overrides the fetch offsets that the consumer will use on the next poll.
    # @param [Hash] topicPartition topic partition for which seek
    # @param [Fixnum] offset offset to seek inside the topic partition
    # @yield handler called on operation completed
    # @return [self]
    def seek(topicPartition=nil,offset=nil)
      if topicPartition.class == Hash && offset.class == Fixnum && !block_given?
        @j_del.java_method(:seek, [Java::IoVertxKafkaClientCommon::TopicPartition.java_class,Java::long.java_class]).call(Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(topicPartition)),offset)
        return self
      elsif topicPartition.class == Hash && offset.class == Fixnum && block_given?
        @j_del.java_method(:seek, [Java::IoVertxKafkaClientCommon::TopicPartition.java_class,Java::long.java_class,Java::IoVertxCore::Handler.java_class]).call(Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(topicPartition)),offset,(Proc.new { |ar| yield(ar.failed ? ar.cause : nil) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling seek(#{topicPartition},#{offset})"
    end
    #  Seek to the first offset for each of the given partitions.
    # @overload seekToBeginning(topicPartition)
    #   @param [Hash] topicPartition topic partition for which seek
    # @overload seekToBeginning(topicPartitions)
    #   @param [Set<Hash>] topicPartitions topic partition for which seek
    # @overload seekToBeginning(topicPartition,completionHandler)
    #   @param [Hash] topicPartition topic partition for which seek
    #   @yield handler called on operation completed
    # @overload seekToBeginning(topicPartitions,completionHandler)
    #   @param [Set<Hash>] topicPartitions topic partition for which seek
    #   @yield handler called on operation completed
    # @return [self]
    def seek_to_beginning(param_1=nil)
      if param_1.class == Hash && !block_given?
        @j_del.java_method(:seekToBeginning, [Java::IoVertxKafkaClientCommon::TopicPartition.java_class]).call(Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(param_1)))
        return self
      elsif param_1.class == Set && !block_given?
        @j_del.java_method(:seekToBeginning, [Java::JavaUtil::Set.java_class]).call(Java::JavaUtil::LinkedHashSet.new(param_1.map { |element| Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(element)) }))
        return self
      elsif param_1.class == Hash && block_given?
        @j_del.java_method(:seekToBeginning, [Java::IoVertxKafkaClientCommon::TopicPartition.java_class,Java::IoVertxCore::Handler.java_class]).call(Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(param_1)),(Proc.new { |ar| yield(ar.failed ? ar.cause : nil) }))
        return self
      elsif param_1.class == Set && block_given?
        @j_del.java_method(:seekToBeginning, [Java::JavaUtil::Set.java_class,Java::IoVertxCore::Handler.java_class]).call(Java::JavaUtil::LinkedHashSet.new(param_1.map { |element| Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(element)) }),(Proc.new { |ar| yield(ar.failed ? ar.cause : nil) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling seek_to_beginning(#{param_1})"
    end
    #  Seek to the last offset for each of the given partitions.
    # @overload seekToEnd(topicPartition)
    #   @param [Hash] topicPartition topic partition for which seek
    # @overload seekToEnd(topicPartitions)
    #   @param [Set<Hash>] topicPartitions topic partition for which seek
    # @overload seekToEnd(topicPartition,completionHandler)
    #   @param [Hash] topicPartition topic partition for which seek
    #   @yield handler called on operation completed
    # @overload seekToEnd(topicPartitions,completionHandler)
    #   @param [Set<Hash>] topicPartitions topic partition for which seek
    #   @yield handler called on operation completed
    # @return [self]
    def seek_to_end(param_1=nil)
      if param_1.class == Hash && !block_given?
        @j_del.java_method(:seekToEnd, [Java::IoVertxKafkaClientCommon::TopicPartition.java_class]).call(Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(param_1)))
        return self
      elsif param_1.class == Set && !block_given?
        @j_del.java_method(:seekToEnd, [Java::JavaUtil::Set.java_class]).call(Java::JavaUtil::LinkedHashSet.new(param_1.map { |element| Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(element)) }))
        return self
      elsif param_1.class == Hash && block_given?
        @j_del.java_method(:seekToEnd, [Java::IoVertxKafkaClientCommon::TopicPartition.java_class,Java::IoVertxCore::Handler.java_class]).call(Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(param_1)),(Proc.new { |ar| yield(ar.failed ? ar.cause : nil) }))
        return self
      elsif param_1.class == Set && block_given?
        @j_del.java_method(:seekToEnd, [Java::JavaUtil::Set.java_class,Java::IoVertxCore::Handler.java_class]).call(Java::JavaUtil::LinkedHashSet.new(param_1.map { |element| Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(element)) }),(Proc.new { |ar| yield(ar.failed ? ar.cause : nil) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling seek_to_end(#{param_1})"
    end
    #  Commit current offsets for all the subscribed list of topics and partition.
    # @yield handler called on operation completed
    # @return [void]
    def commit
      if !block_given?
        return @j_del.java_method(:commit, []).call()
      elsif block_given?
        return @j_del.java_method(:commit, [Java::IoVertxCore::Handler.java_class]).call((Proc.new { |ar| yield(ar.failed ? ar.cause : nil) }))
      end
      raise ArgumentError, "Invalid arguments when calling commit()"
    end
    #  Get the last committed offset for the given partition (whether the commit happened by this process or another).
    # @param [Hash] topicPartition topic partition for getting last committed offset
    # @yield handler called on operation completed
    # @return [void]
    def committed(topicPartition=nil)
      if topicPartition.class == Hash && block_given?
        return @j_del.java_method(:committed, [Java::IoVertxKafkaClientCommon::TopicPartition.java_class,Java::IoVertxCore::Handler.java_class]).call(Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(topicPartition)),(Proc.new { |ar| yield(ar.failed ? ar.cause : nil, ar.succeeded ? ar.result != nil ? JSON.parse(ar.result.toJson.encode) : nil : nil) }))
      end
      raise ArgumentError, "Invalid arguments when calling committed(#{topicPartition})"
    end
    #  Get metadata about the partitions for a given topic.
    # @param [String] topic topic partition for which getting partitions info
    # @yield handler called on operation completed
    # @return [self]
    def partitions_for(topic=nil)
      if topic.class == String && block_given?
        @j_del.java_method(:partitionsFor, [Java::java.lang.String.java_class,Java::IoVertxCore::Handler.java_class]).call(topic,(Proc.new { |ar| yield(ar.failed ? ar.cause : nil, ar.succeeded ? ar.result.to_a.map { |elt| elt != nil ? JSON.parse(elt.toJson.encode) : nil } : nil) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling partitions_for(#{topic})"
    end
    #  Close the consumer
    # @yield handler called on operation completed
    # @return [void]
    def close
      if !block_given?
        return @j_del.java_method(:close, []).call()
      elsif block_given?
        return @j_del.java_method(:close, [Java::IoVertxCore::Handler.java_class]).call(Proc.new { yield })
      end
      raise ArgumentError, "Invalid arguments when calling close()"
    end
    #  Get the offset of the next record that will be fetched (if a record with that offset exists).
    # @param [Hash] partition The partition to get the position for
    # @yield handler called on operation completed
    # @return [void]
    def position(partition=nil)
      if partition.class == Hash && block_given?
        return @j_del.java_method(:position, [Java::IoVertxKafkaClientCommon::TopicPartition.java_class,Java::IoVertxCore::Handler.java_class]).call(Java::IoVertxKafkaClientCommon::TopicPartition.new(::Vertx::Util::Utils.to_json_object(partition)),(Proc.new { |ar| yield(ar.failed ? ar.cause : nil, ar.succeeded ? ar.result : nil) }))
      end
      raise ArgumentError, "Invalid arguments when calling position(#{partition})"
    end
  end
end

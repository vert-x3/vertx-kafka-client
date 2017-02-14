require 'vertx/write_stream'
require 'vertx/vertx'
require 'vertx-kafka-client/kafka_producer_record'
require 'vertx/util/utils.rb'
# Generated from io.vertx.kafka.client.producer.KafkaProducer
module VertxKafkaClient
  #  Vert.x Kafka producer.
  #  <p>
  #  The  provides global control over writing a record.
  class KafkaProducer
    include ::Vertx::WriteStream
    # @private
    # @param j_del [::VertxKafkaClient::KafkaProducer] the java delegate
    def initialize(j_del, j_arg_K=nil, j_arg_V=nil)
      @j_del = j_del
      @j_arg_K = j_arg_K != nil ? j_arg_K : ::Vertx::Util::unknown_type
      @j_arg_V = j_arg_V != nil ? j_arg_V : ::Vertx::Util::unknown_type
    end
    # @private
    # @return [::VertxKafkaClient::KafkaProducer] the underlying java delegate
    def j_del
      @j_del
    end
    #  Create a new KafkaProducer instance
    # @param [::Vertx::Vertx] vertx Vert.x instance to use
    # @param [Hash{String => String}] config Kafka producer configuration
    # @param [Nil] keyType class type for the key serialization
    # @param [Nil] valueType class type for the value serialization
    # @return [::VertxKafkaClient::KafkaProducer] an instance of the KafkaProducer
    def self.create(vertx=nil,config=nil,keyType=nil,valueType=nil)
      if vertx.class.method_defined?(:j_del) && config.class == Hash && !block_given? && keyType == nil && valueType == nil
        return ::Vertx::Util::Utils.safe_create(Java::IoVertxKafkaClientProducer::KafkaProducer.java_method(:create, [Java::IoVertxCore::Vertx.java_class,Java::JavaUtil::Map.java_class]).call(vertx.j_del,Hash[config.map { |k,v| [k,v] }]),::VertxKafkaClient::KafkaProducer, nil, nil)
      elsif vertx.class.method_defined?(:j_del) && config.class == Hash && keyType.class == Class && valueType.class == Class && !block_given?
        return ::Vertx::Util::Utils.safe_create(Java::IoVertxKafkaClientProducer::KafkaProducer.java_method(:create, [Java::IoVertxCore::Vertx.java_class,Java::JavaUtil::Map.java_class,Java::JavaLang::Class.java_class,Java::JavaLang::Class.java_class]).call(vertx.j_del,Hash[config.map { |k,v| [k,v] }],::Vertx::Util::Utils.j_class_of(keyType),::Vertx::Util::Utils.j_class_of(valueType)),::VertxKafkaClient::KafkaProducer, ::Vertx::Util::Utils.v_type_of(keyType), ::Vertx::Util::Utils.v_type_of(valueType))
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
    #  Asynchronously write a record to a topic
    # @param [::VertxKafkaClient::KafkaProducerRecord] record record to write
    # @yield handler called on operation completed
    # @return [self]
    def write(record=nil)
      if record.class.method_defined?(:j_del) && !block_given?
        @j_del.java_method(:write, [Java::IoVertxKafkaClientProducer::KafkaProducerRecord.java_class]).call(record.j_del)
        return self
      elsif record.class.method_defined?(:j_del) && block_given?
        @j_del.java_method(:write, [Java::IoVertxKafkaClientProducer::KafkaProducerRecord.java_class,Java::IoVertxCore::Handler.java_class]).call(record.j_del,(Proc.new { |ar| yield(ar.failed ? ar.cause : nil, ar.succeeded ? ar.result != nil ? JSON.parse(ar.result.toJson.encode) : nil : nil) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling write(#{record})"
    end
    # @param [::VertxKafkaClient::KafkaProducerRecord] kafkaProducerRecord 
    # @return [void]
    def end(kafkaProducerRecord=nil)
      if !block_given? && kafkaProducerRecord == nil
        return @j_del.java_method(:end, []).call()
      elsif kafkaProducerRecord.class.method_defined?(:j_del) && !block_given?
        return @j_del.java_method(:end, [Java::IoVertxKafkaClientProducer::KafkaProducerRecord.java_class]).call(kafkaProducerRecord.j_del)
      end
      raise ArgumentError, "Invalid arguments when calling end(#{kafkaProducerRecord})"
    end
    # @param [Fixnum] i 
    # @return [self]
    def set_write_queue_max_size(i=nil)
      if i.class == Fixnum && !block_given?
        @j_del.java_method(:setWriteQueueMaxSize, [Java::int.java_class]).call(i)
        return self
      end
      raise ArgumentError, "Invalid arguments when calling set_write_queue_max_size(#{i})"
    end
    # @return [true,false]
    def write_queue_full?
      if !block_given?
        return @j_del.java_method(:writeQueueFull, []).call()
      end
      raise ArgumentError, "Invalid arguments when calling write_queue_full?()"
    end
    # @yield 
    # @return [self]
    def drain_handler
      if block_given?
        @j_del.java_method(:drainHandler, [Java::IoVertxCore::Handler.java_class]).call(Proc.new { yield })
        return self
      end
      raise ArgumentError, "Invalid arguments when calling drain_handler()"
    end
    #  Get the partition metadata for the give topic.
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
    #  Invoking this method makes all buffered records immediately available to write
    # @yield handler called on operation completed
    # @return [self]
    def flush
      if block_given?
        @j_del.java_method(:flush, [Java::IoVertxCore::Handler.java_class]).call(Proc.new { yield })
        return self
      end
      raise ArgumentError, "Invalid arguments when calling flush()"
    end
    #  Close the producer
    # @param [Fixnum] timeout timeout to wait for closing
    # @yield handler called on operation completed
    # @return [void]
    def close(timeout=nil)
      if !block_given? && timeout == nil
        return @j_del.java_method(:close, []).call()
      elsif timeout.class == Fixnum && block_given?
        return @j_del.java_method(:close, [Java::long.java_class,Java::IoVertxCore::Handler.java_class]).call(timeout,Proc.new { yield })
      end
      raise ArgumentError, "Invalid arguments when calling close(#{timeout})"
    end
  end
end

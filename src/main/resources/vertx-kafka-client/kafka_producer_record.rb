require 'vertx/util/utils.rb'
# Generated from io.vertx.kafka.client.producer.KafkaProducerRecord
module VertxKafkaClient
  #  Vert.x Kafka producer record
  class KafkaProducerRecord
    # @private
    # @param j_del [::VertxKafkaClient::KafkaProducerRecord] the java delegate
    def initialize(j_del, j_arg_K=nil, j_arg_V=nil)
      @j_del = j_del
      @j_arg_K = j_arg_K != nil ? j_arg_K : ::Vertx::Util::unknown_type
      @j_arg_V = j_arg_V != nil ? j_arg_V : ::Vertx::Util::unknown_type
    end
    # @private
    # @return [::VertxKafkaClient::KafkaProducerRecord] the underlying java delegate
    def j_del
      @j_del
    end
    #  Create a concrete instance of a Vert.x producer record
    # @overload create(topic,value)
    #   @param [String] topic the topic this record is being sent to
    #   @param [Object] value the value
    # @overload create(topic,key,value)
    #   @param [String] topic the topic this record is being sent to
    #   @param [Object] key the key (or null if no key is specified)
    #   @param [Object] value the value
    # @overload create(topic,key,value,timestamp,partition)
    #   @param [String] topic the topic this record is being sent to
    #   @param [Object] key the key (or null if no key is specified)
    #   @param [Object] value the value
    #   @param [Fixnum] timestamp the timestamp of this record
    #   @param [Fixnum] partition the partition to which the record will be sent (or null if no partition was specified)
    # @return [::VertxKafkaClient::KafkaProducerRecord] Vert.x producer record
    def self.create(param_1=nil,param_2=nil,param_3=nil,param_4=nil,param_5=nil)
      if param_1.class == String && ::Vertx::Util::unknown_type.accept?(param_2) && !block_given? && param_3 == nil && param_4 == nil && param_5 == nil
        return ::Vertx::Util::Utils.safe_create(Java::IoVertxKafkaClientProducer::KafkaProducerRecord.java_method(:create, [Java::java.lang.String.java_class,Java::java.lang.Object.java_class]).call(param_1,::Vertx::Util::Utils.to_object(param_2)),::VertxKafkaClient::KafkaProducerRecord, nil, nil)
      elsif param_1.class == String && ::Vertx::Util::unknown_type.accept?(param_2) && ::Vertx::Util::unknown_type.accept?(param_3) && !block_given? && param_4 == nil && param_5 == nil
        return ::Vertx::Util::Utils.safe_create(Java::IoVertxKafkaClientProducer::KafkaProducerRecord.java_method(:create, [Java::java.lang.String.java_class,Java::java.lang.Object.java_class,Java::java.lang.Object.java_class]).call(param_1,::Vertx::Util::Utils.to_object(param_2),::Vertx::Util::Utils.to_object(param_3)),::VertxKafkaClient::KafkaProducerRecord, nil, nil)
      elsif param_1.class == String && ::Vertx::Util::unknown_type.accept?(param_2) && ::Vertx::Util::unknown_type.accept?(param_3) && param_4.class == Fixnum && param_5.class == Fixnum && !block_given?
        return ::Vertx::Util::Utils.safe_create(Java::IoVertxKafkaClientProducer::KafkaProducerRecord.java_method(:create, [Java::java.lang.String.java_class,Java::java.lang.Object.java_class,Java::java.lang.Object.java_class,Java::JavaLang::Long.java_class,Java::JavaLang::Integer.java_class]).call(param_1,::Vertx::Util::Utils.to_object(param_2),::Vertx::Util::Utils.to_object(param_3),param_4,::Vertx::Util::Utils.to_integer(param_5)),::VertxKafkaClient::KafkaProducerRecord, nil, nil)
      end
      raise ArgumentError, "Invalid arguments when calling create(#{param_1},#{param_2},#{param_3},#{param_4},#{param_5})"
    end
    # @return [String] the topic this record is being sent to
    def topic
      if !block_given?
        return @j_del.java_method(:topic, []).call()
      end
      raise ArgumentError, "Invalid arguments when calling topic()"
    end
    # @return [Object] the key (or null if no key is specified)
    def key
      if !block_given?
        return @j_arg_K.wrap(@j_del.java_method(:key, []).call())
      end
      raise ArgumentError, "Invalid arguments when calling key()"
    end
    # @return [Object] the value
    def value
      if !block_given?
        return @j_arg_V.wrap(@j_del.java_method(:value, []).call())
      end
      raise ArgumentError, "Invalid arguments when calling value()"
    end
    # @return [Fixnum] the timestamp of this record
    def timestamp
      if !block_given?
        return @j_del.java_method(:timestamp, []).call()
      end
      raise ArgumentError, "Invalid arguments when calling timestamp()"
    end
    # @return [Fixnum] the partition to which the record will be sent (or null if no partition was specified)
    def partition
      if !block_given?
        return @j_del.java_method(:partition, []).call()
      end
      raise ArgumentError, "Invalid arguments when calling partition()"
    end
  end
end

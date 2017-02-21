require 'vertx/util/utils.rb'
# Generated from io.vertx.kafka.client.consumer.KafkaConsumerRecord
module VertxKafkaClient
  #  Vert.x Kafka consumer record
  class KafkaConsumerRecord
    # @private
    # @param j_del [::VertxKafkaClient::KafkaConsumerRecord] the java delegate
    def initialize(j_del, j_arg_K=nil, j_arg_V=nil)
      @j_del = j_del
      @j_arg_K = j_arg_K != nil ? j_arg_K : ::Vertx::Util::unknown_type
      @j_arg_V = j_arg_V != nil ? j_arg_V : ::Vertx::Util::unknown_type
    end
    # @private
    # @return [::VertxKafkaClient::KafkaConsumerRecord] the underlying java delegate
    def j_del
      @j_del
    end
    # @return [String] the topic this record is received from
    def topic
      if !block_given?
        return @j_del.java_method(:topic, []).call()
      end
      raise ArgumentError, "Invalid arguments when calling topic()"
    end
    # @return [Fixnum] the partition from which this record is received
    def partition
      if !block_given?
        return @j_del.java_method(:partition, []).call()
      end
      raise ArgumentError, "Invalid arguments when calling partition()"
    end
    # @return [Fixnum] the position of this record in the corresponding Kafka partition.
    def offset
      if !block_given?
        return @j_del.java_method(:offset, []).call()
      end
      raise ArgumentError, "Invalid arguments when calling offset()"
    end
    # @return [Fixnum] the timestamp of this record
    def timestamp
      if !block_given?
        return @j_del.java_method(:timestamp, []).call()
      end
      raise ArgumentError, "Invalid arguments when calling timestamp()"
    end
    # @return [:NO_TIMESTAMP_TYPE,:CREATE_TIME,:LOG_APPEND_TIME] the timestamp type of this record
    def timestamp_type
      if !block_given?
        return @j_del.java_method(:timestampType, []).call().name.intern
      end
      raise ArgumentError, "Invalid arguments when calling timestamp_type()"
    end
    # @return [Fixnum] the checksum (CRC32) of the record.
    def checksum
      if !block_given?
        return @j_del.java_method(:checksum, []).call()
      end
      raise ArgumentError, "Invalid arguments when calling checksum()"
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
  end
end

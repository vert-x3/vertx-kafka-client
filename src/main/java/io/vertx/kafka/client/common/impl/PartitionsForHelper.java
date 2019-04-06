package io.vertx.kafka.client.common.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.kafka.client.common.PartitionInfo;

public class PartitionsForHelper {

  public static String getNoSuchTopicFailMessageFormat(final String topic) {
    return String.format("There is no such topic '%s'. Check if auto.create.topics.enable is set to true.", topic);
  }

  public static void partitionsFor(final String topic, final Handler<AsyncResult<List<PartitionInfo>>> handler, final AsyncResult<List<org.apache.kafka.common.PartitionInfo>> done) {
    if (done.succeeded()) {
      // TODO: use Helper class and stream approach
      List<PartitionInfo> partitions = new ArrayList<>();
      List<org.apache.kafka.common.PartitionInfo> partitionInfos = done.result();
      if (partitionInfos == null) {
        handler.handle(Future.failedFuture(getNoSuchTopicFailMessageFormat(topic)));
        return;
      }
      for (org.apache.kafka.common.PartitionInfo kafkaPartitionInfo : partitionInfos) {

        PartitionInfo partitionInfo = PartitionsForHelper.fillPartitionInfo(kafkaPartitionInfo);

        partitions.add(partitionInfo);
      }
      handler.handle(Future.succeededFuture(partitions));
    } else {
      handler.handle(Future.failedFuture(done.cause()));
    }
  }

  public static PartitionInfo fillPartitionInfo(final org.apache.kafka.common.PartitionInfo kafkaPartitionInfo) {
    PartitionInfo partitionInfo = new PartitionInfo();

    partitionInfo
      .setInSyncReplicas(
        Stream.of(kafkaPartitionInfo.inSyncReplicas()).map(Helper::from).collect(Collectors.toList()))
      .setLeader(Helper.from(kafkaPartitionInfo.leader()))
      .setPartition(kafkaPartitionInfo.partition())
      .setReplicas(
        Stream.of(kafkaPartitionInfo.replicas()).map(Helper::from).collect(Collectors.toList()))
      .setTopic(kafkaPartitionInfo.topic());
    return partitionInfo;
  }
}

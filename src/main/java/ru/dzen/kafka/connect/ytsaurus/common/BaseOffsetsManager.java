package ru.dzen.kafka.connect.ytsaurus.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import tech.ytsaurus.client.ApiServiceTransaction;

public abstract class BaseOffsetsManager {

  public Collection<SinkRecord> filterRecords(Collection<SinkRecord> sinkRecords,
      Map<TopicPartition, OffsetAndMetadata> prevOffsetsMap) {
    Collection<SinkRecord> res = new ArrayList<>();
    for (var record : sinkRecords) {
      var topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
      if (!prevOffsetsMap.containsKey(topicPartition)
          || prevOffsetsMap.get(topicPartition).offset() < record.kafkaOffset()) {
        res.add(record);
      }
    }
    return res;
  }

  public Map<TopicPartition, OffsetAndMetadata> getMaxOffsets(Collection<SinkRecord> sinkRecords) {
    Map<TopicPartition, OffsetAndMetadata> res = new HashMap<>();
    for (var record : sinkRecords) {
      var topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
      if (!res.containsKey(topicPartition)
          || res.get(topicPartition).offset() < record.kafkaOffset()) {
        res.put(topicPartition, new OffsetAndMetadata(record.kafkaOffset()));
      }
    }
    return res;
  }

  public abstract Map<TopicPartition, OffsetAndMetadata> getPrevOffsets(ApiServiceTransaction trx,
      Set<TopicPartition> topicPartitions)
      throws InterruptedException, java.util.concurrent.ExecutionException;

  public abstract void writeOffsets(ApiServiceTransaction trx,
      Map<TopicPartition, OffsetAndMetadata> offsets)
      throws InterruptedException, java.util.concurrent.ExecutionException;

  public abstract void lockPartitions(ApiServiceTransaction trx,
      Set<TopicPartition> partitions)
      throws InterruptedException, java.util.concurrent.ExecutionException;
}

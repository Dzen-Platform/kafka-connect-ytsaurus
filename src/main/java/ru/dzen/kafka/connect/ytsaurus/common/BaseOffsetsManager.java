package ru.dzen.kafka.connect.ytsaurus.common;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import tech.ytsaurus.client.ApiServiceTransaction;

public abstract class BaseOffsetsManager {

  public Collection<SinkRecord> filterRecords(Collection<SinkRecord> sinkRecords,
      Map<TopicPartition, OffsetAndMetadata> committedOffsets) {
    return sinkRecords.stream()
        .filter(record -> {
          var topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
          return !committedOffsets.containsKey(topicPartition)
              || committedOffsets.get(topicPartition).offset() < record.kafkaOffset();
        })
        .collect(Collectors.toList());
  }

  public Map<TopicPartition, OffsetAndMetadata> getMaxOffsets(Collection<SinkRecord> sinkRecords) {
    return sinkRecords.stream()
        .collect(Collectors.toMap(
            record -> new TopicPartition(record.topic(), record.kafkaPartition()),
            record -> new OffsetAndMetadata(record.kafkaOffset()),
            (prev, curr) -> curr.offset() > prev.offset() ? curr : prev
        ));
  }

  public abstract Map<TopicPartition, OffsetAndMetadata> getCommittedOffsets(ApiServiceTransaction trx,
      Set<TopicPartition> topicPartitions)
      throws InterruptedException, java.util.concurrent.ExecutionException;

  public abstract void writeOffsets(ApiServiceTransaction trx,
      Map<TopicPartition, OffsetAndMetadata> offsets)
      throws InterruptedException, java.util.concurrent.ExecutionException;

  public abstract void lockPartitions(ApiServiceTransaction trx,
      Set<TopicPartition> partitions)
      throws InterruptedException, java.util.concurrent.ExecutionException;
}

package ru.dzen.kafka.connect.ytsaurus.dynamicTable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import ru.dzen.kafka.connect.ytsaurus.common.BaseOffsetsManager;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema.EColumn;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.LookupRowsRequest;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.core.cypress.YPath;

public class DynTableOffsetsManager extends BaseOffsetsManager {

  private final YPath pathToOffsetsTable;

  DynTableOffsetsManager(YPath pathToOffsetsTable) {
    this.pathToOffsetsTable = pathToOffsetsTable;
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> getPrevOffsets(ApiServiceTransaction trx,
      Set<TopicPartition> topicPartitions) throws InterruptedException, ExecutionException {
    var requestBuilder = LookupRowsRequest.builder()
        .setPath(pathToOffsetsTable.toString())
        .setSchema(UnstructuredTableSchema.OFFSETS_TABLE_SCHEMA.toLookup())
        .addLookupColumns(UnstructuredTableSchema.EColumn.TOPIC.name,
            UnstructuredTableSchema.EColumn.PARTITION.name,
            UnstructuredTableSchema.EColumn.OFFSET.name);
    for (var topicPartition : topicPartitions) {
      requestBuilder = requestBuilder.addFilter(topicPartition.topic(), topicPartition.partition());
    }
    var prevOffsetsRows = trx.lookupRows(requestBuilder.build())
        .get().getRows().stream();
    return prevOffsetsRows.collect(Collectors.toMap(
        row -> new TopicPartition(row.getValues().get(0).stringValue(),
            (int) row.getValues().get(1).longValue()),
        row -> new OffsetAndMetadata(row.getValues().get(2).longValue()),
        (v1, v2) -> v1,
        HashMap::new));
  }


  @Override
  public void writeOffsets(ApiServiceTransaction trx,
      Map<TopicPartition, OffsetAndMetadata> offsets)
      throws InterruptedException, ExecutionException {
    var modifyRowsRequestBuilder = ModifyRowsRequest.builder()
        .setPath(pathToOffsetsTable.toString())
        .setSchema(UnstructuredTableSchema.OFFSETS_TABLE_SCHEMA);
    offsets.entrySet().stream()
        .map(entry -> Map.of(
            EColumn.TOPIC.name, entry.getKey().topic(),
            EColumn.PARTITION.name, entry.getKey().partition(),
            EColumn.OFFSET.name, entry.getValue().offset()
        ))
        .forEach(update -> modifyRowsRequestBuilder.addUpdate(update));
    trx.modifyRows(modifyRowsRequestBuilder.build()).get();
  }

  @Override
  public void lockPartitions(ApiServiceTransaction trx, Set<TopicPartition> topicPartitions)
      throws InterruptedException, ExecutionException {
// Dynamic tables prevent simultaneous modification of one and the same row by default
  }
}

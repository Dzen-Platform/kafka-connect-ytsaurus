package ru.dzen.kafka.connect.ytsaurus.dynamic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import ru.dzen.kafka.connect.ytsaurus.common.BaseOffsetsManager;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema;
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
        .setSchema(UnstructuredTableSchema.offsetsTableSchema.toLookup());
    for (var topicPartition : topicPartitions) {
      requestBuilder = requestBuilder.addFilter(topicPartition.topic(), topicPartition.partition());
    }
    requestBuilder = requestBuilder.addLookupColumns(UnstructuredTableSchema.EColumn.TOPIC.name,
        UnstructuredTableSchema.EColumn.PARTITION.name,
        UnstructuredTableSchema.EColumn.OFFSET.name);
    var prevOffsetsRows = trx.lookupRows(requestBuilder.build()).get().getRows();
    var res = new HashMap<TopicPartition, OffsetAndMetadata>();
    for (var row : prevOffsetsRows) {
      var rowValues = row.getValues();
      res.put(
          new TopicPartition(rowValues.get(0).stringValue(), (int) rowValues.get(1).longValue()),
          new OffsetAndMetadata(rowValues.get(2).longValue()));
    }
    return res;
  }


  @Override
  public void writeOffsets(ApiServiceTransaction trx,
      Map<TopicPartition, OffsetAndMetadata> offsets)
      throws InterruptedException, ExecutionException {
    var modifyRowsRequestBuilder = ModifyRowsRequest.builder()
        .setPath(pathToOffsetsTable.toString())
        .setSchema(UnstructuredTableSchema.offsetsTableSchema);
    for (var entry : offsets.entrySet()) {
      modifyRowsRequestBuilder = modifyRowsRequestBuilder.addUpdate(List.of(
          entry.getKey().topic(),
          entry.getKey().partition(),
          entry.getValue().offset()
      ));
    }
    trx.modifyRows(modifyRowsRequestBuilder.build()).get();
  }

  @Override
  public void lockPartitions(ApiServiceTransaction trx, Set<TopicPartition> topicPartitions)
      throws InterruptedException, ExecutionException {
// Dynamic tables prevent simultaneous modification of one and the same row by default
  }
}

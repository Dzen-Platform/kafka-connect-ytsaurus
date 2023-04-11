package ru.dzen.kafka.connect.ytsaurus.dynamicTable;

import java.util.Collection;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriter;
import ru.dzen.kafka.connect.ytsaurus.common.TableWriterManager;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema.EColumn;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema.ETableType;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.client.request.StartTransaction;

public class DynTableWriter extends BaseTableWriter {

  protected static final Logger log = LoggerFactory.getLogger(DynTableWriter.class);

  public final DynTableWriterConfig config;

  public DynTableWriter(DynTableWriterConfig config) {
    super(config, new DynTableOffsetsManager(config.getOffsetsTablePath()));
    this.config = config;
  }

  @Override
  protected void writeRows(ApiServiceTransaction trx, Collection<SinkRecord> records)
      throws Exception {
    var modifyRowsRequestBuilder = ModifyRowsRequest.builder()
        .setPath(config.getDataQueueTablePath().toString())
        .setSchema(UnstructuredTableSchema.createDataQueueTableSchema(config.getKeyOutputFormat(),
            config.getValueOutputFormat(), EColumn.getAllMetadataColumns(ETableType.DYNAMIC)));
    var mapNodesToWrite = recordsToRows(records);
    for (var mapNodeToWrite : mapNodesToWrite) {
      modifyRowsRequestBuilder = modifyRowsRequestBuilder.addUpdate(mapNodeToWrite);
    }
    trx.modifyRows(modifyRowsRequestBuilder.build()).get();
  }

  @Override
  protected ApiServiceTransaction createTransaction() throws Exception {
    return client.startTransaction(StartTransaction.tablet()).get();
  }

  @Override
  public TableWriterManager getManager() {
    return new DynTableWriterManager(config);
  }
}

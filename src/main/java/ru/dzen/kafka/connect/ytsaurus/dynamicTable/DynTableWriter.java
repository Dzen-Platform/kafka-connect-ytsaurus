package ru.dzen.kafka.connect.ytsaurus.dynamicTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.ExtractField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriter;
import ru.dzen.kafka.connect.ytsaurus.common.TableWriterManager;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.DynTableWriterConfig.UpdateMode;
import ru.dzen.kafka.connect.ytsaurus.table.DyntableRowsDelete;
import ru.dzen.kafka.connect.ytsaurus.table.DyntableRowsInsert;
import ru.dzen.kafka.connect.ytsaurus.table.DyntableRowsUpdate;
import ru.dzen.kafka.connect.ytsaurus.table.Operation;
import ru.dzen.kafka.connect.ytsaurus.table.Update;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.StartTransaction;

public class DynTableWriter extends BaseTableWriter {

  private static final Logger log = LoggerFactory.getLogger(DynTableWriter.class);

  public final DynTableWriterConfig config;
  private final ExtractField.Value<SinkRecord> operationExtractor;

  public DynTableWriter(DynTableWriterConfig config) {
    super(config, new DynTableOffsetsManager(config.getOffsetsTablePath()));
    this.config = config;
    this.operationExtractor = new ExtractField.Value<>();
    final Map<String, String> operationExtractorConfig = new HashMap<>();
    operationExtractorConfig.put("field", "op");
    operationExtractor.configure(operationExtractorConfig);
  }

  @Override
  protected void writeRows(ApiServiceTransaction trx, Collection<SinkRecord> records)
      throws Exception {
    if (records.isEmpty()) {
      return;
    }

    Operation currentOperation = null;
    List<SinkRecord> affectedRecords = new ArrayList<>();
    for (SinkRecord record : records) {
      Operation operation = getOperationType(record);
      if (currentOperation == null) {
        currentOperation = operation;
      }
      if (currentOperation == operation) {
        affectedRecords.add(record);
        continue;
      }
      Update update = createUpdate(currentOperation, affectedRecords);
      update.execute(trx);
      currentOperation = operation;
      affectedRecords.clear();
      affectedRecords.add(record);
    }
    Update update = createUpdate(currentOperation, affectedRecords);
    update.execute(trx);
  }

  @Override
  protected ApiServiceTransaction createTransaction() throws Exception {
    return client.startTransaction(StartTransaction.tablet()).get();
  }

  @Override
  public TableWriterManager getManager() {
    return new DynTableWriterManager(config);
  }

  private Operation getOperationType(SinkRecord record) {
    if (config.getUpdateMode() == UpdateMode.INSERTS) {
      return Operation.CREATE;
    }
    String operationLiteral = operationExtractor.apply(record).value().toString();
    return Operation.byLiteral(operationLiteral).orElseThrow();
  }

  private Update createUpdate(Operation operation, List<SinkRecord> affectedRecords) {
    switch (operation) {
      case CREATE:
      case READ:
        return createInsertRowsUpdate(affectedRecords);
      case UPDATE:
        return createUpdateRowsUpdate(affectedRecords);
      case DELETE:
        return createDeleteRowsUpdate(affectedRecords);
      default:
        throw new UnsupportedOperationException(
            String.format("Operation type %s is not supported", operation)
        );
    }
  }

  private Update createInsertRowsUpdate(List<SinkRecord> affectedRecords) {
    return new DyntableRowsInsert(config.getDataQueueTablePath(), recordsToRows(affectedRecords));
  }

  private Update createUpdateRowsUpdate(List<SinkRecord> affectedRecords) {
    return new DyntableRowsUpdate(config.getDataQueueTablePath(), recordsToRows(affectedRecords));
  }

  private Update createDeleteRowsUpdate(List<SinkRecord> affectedRecords) {
    return new DyntableRowsDelete(config.getDataQueueTablePath(), recordsToRows(affectedRecords));
  }
}

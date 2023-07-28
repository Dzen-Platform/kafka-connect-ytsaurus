package ru.dzen.kafka.connect.ytsaurus.dynamicTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.ExtractField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriter;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputType;
import ru.dzen.kafka.connect.ytsaurus.common.TableRow;
import ru.dzen.kafka.connect.ytsaurus.common.TableWriterManager;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.RecordRouter.Destination;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations.DyntableCreate;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations.DyntableRowsDelete;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations.DyntableRowsInsert;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations.DyntableRowsUpdate;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations.DyntableSchemaUpdate;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations.Operation;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations.TableOperation;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations.TableSchemaRead;
import tech.ytsaurus.client.ApiServiceClient;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.request.TransactionType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.TableSchema;

public class DynTableWriter extends BaseTableWriter {

  private static final Logger log = LoggerFactory.getLogger(DynTableWriter.class);

  public final DynTableWriterConfig config;
  private final ExtractField.Value<SinkRecord> operationExtractor;
  private final RecordRouter recordRouter;

  public DynTableWriter(DynTableWriterConfig config) {
    super(config, new DynTableOffsetsManager(config.getOffsetsTablePath()));
    this.config = config;
    this.operationExtractor = new ExtractField.Value<>();
    final Map<String, String> operationExtractorConfig = new HashMap<>();
    operationExtractorConfig.put("field", config.getOperationField());
    operationExtractor.configure(operationExtractorConfig);
    this.recordRouter = new RecordRouter(config);
  }

  @Override
  protected void writeRows(ApiServiceTransaction trx, Collection<SinkRecord> records)
      throws Exception {
    if (records.isEmpty()) {
      return;
    }
    if (config.isTableRouterEnabled()) {
      List<Destination> destinations = recordRouter.route(records);
      for (Destination dst : destinations) {
        doWriteRows(trx, dst.getDestinationPath(), dst.getRecords());
      }
    } else {
      doWriteRows(trx, config.getDataQueueTablePath(), records);
    }
  }

  private void doWriteRows(
      ApiServiceTransaction tx,
      YPath tablePath,
      Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }
    TableSchema currentSchema = null;
    Operation currentOperation = null;
    List<TableRow> affectedRows = new ArrayList<>();
    for (SinkRecord record : records) {
      TableRow tableRow = tableRowMapper.recordToRow(record);
      if (currentSchema == null) {
        currentSchema = getInTransaction(tx.getClient(), TransactionType.Master, t -> {
          if (!t.existsNode(tablePath.toString()).join()) {
            DyntableCreate dyntableCreate = new DyntableCreate(
                tablePath,
                tableRow.getSchema(),
                config.getTableExtraAttributes());
            dyntableCreate.execute(t);
          }
          TableSchemaRead schemaReadOp = new TableSchemaRead(tablePath);
          schemaReadOp.execute(t);
          return schemaReadOp.getSchema();
        });
        DyntableUtils.mountTable(client, tablePath);
      }
      if (schemaUpdateRequired(currentSchema, tableRow.getSchema())) {
        if (currentOperation != null && !affectedRows.isEmpty()) {
          TableOperation update = createUpdate(currentOperation, tablePath, affectedRows);
          runInTransaction(tx.getClient(), TransactionType.Tablet, update::execute);
          currentOperation = null;
          affectedRows.clear();
        }
        DyntableSchemaUpdate schemaUpdate = new DyntableSchemaUpdate(
            tablePath, currentSchema, tableRow);
        currentSchema = getInTransaction(tx.getClient(), TransactionType.Master, t -> {
          schemaUpdate.execute(t);
          return schemaUpdate.getCurrentSchema();
        });
      }
      Operation operation = getOperationType(record);
      if (currentOperation == null) {
        currentOperation = operation;
      }
      if (currentOperation == operation) {
        affectedRows.add(tableRow);
        continue;
      }
      TableOperation update = createUpdate(currentOperation, tablePath, affectedRows);
      runInTransaction(tx.getClient(), TransactionType.Tablet, update::execute);
      currentOperation = operation;
      affectedRows.clear();
      affectedRows.add(tableRow);
    }
    TableOperation update = createUpdate(currentOperation, tablePath, affectedRows);
    runInTransaction(tx.getClient(), TransactionType.Tablet, update::execute);
  }

  @Override
  protected ApiServiceTransaction createTransaction() throws Exception {
    return client.startTransaction(StartTransaction.tablet()).get();
  }

  @Override
  public TableWriterManager getManager() {
    return new DynTableWriterManager(config);
  }

  private void runInTransaction(
      ApiServiceClient client,
      TransactionType txType,
      Consumer<ApiServiceTransaction> txConsumer)
  {
    getInTransaction(client, txType, t -> {
      txConsumer.accept(t);
      return null;
    });
  }

  private <T> T getInTransaction(
      ApiServiceClient client,
      TransactionType txType,
      Function<ApiServiceTransaction, T> getFunction) {
    StartTransaction txReq = StartTransaction.builder()
        .setType(txType)
        .setSticky(txType == TransactionType.Tablet)
        .build();
    T result;
    try (ApiServiceTransaction tx = client.startTransaction(txReq).join()) {
      result = getFunction.apply(tx);
      tx.commit().join();
    }
    return result;
  }

  private boolean schemaUpdateRequired(TableSchema currentSchema, TableSchema rowSchema) {
    for (ColumnSchema col : rowSchema.getColumns()) {
      int colIdx = currentSchema.findColumn(col.getName());
      if (colIdx == -1
          || !currentSchema.getColumnSchema(colIdx).getTypeV3().equals(col.getTypeV3())) {
        return true;
      }
    }
    return false;
  }

  private Operation getOperationType(SinkRecord record) {
    if (config.getOutputType() == OutputType.DYNAMIC_ORDERED_TABLES) {
      return Operation.CREATE;
    }
    return Optional.ofNullable(operationExtractor.apply(record))
        .map(SinkRecord::value)
        .map(Object::toString)
        .flatMap(Operation::byLiteral)
        .orElse(Operation.UPDATE);
  }

  private TableOperation createUpdate(
      Operation operation, YPath tablePath, List<TableRow> affectedRows) {
    switch (operation) {
      case CREATE:
      case READ:
        return createInsertRowsUpdate(tablePath, affectedRows);
      case UPDATE:
        return createUpdateRowsUpdate(tablePath, affectedRows);
      case DELETE:
        return createDeleteRowsUpdate(tablePath, affectedRows);
      default:
        throw new UnsupportedOperationException(
            String.format("Operation type %s is not supported", operation)
        );
    }
  }

  private TableOperation createInsertRowsUpdate(YPath tablePath, List<TableRow> tableRows) {
    return new DyntableRowsInsert(tablePath, tableRows);
  }

  private TableOperation createUpdateRowsUpdate(YPath tablePath, List<TableRow> tableRows) {
    return new DyntableRowsUpdate(tablePath, tableRows);
  }

  private TableOperation createDeleteRowsUpdate(YPath tablePath, List<TableRow> tableRows) {
    return new DyntableRowsDelete(tablePath, tableRows);
  }
}

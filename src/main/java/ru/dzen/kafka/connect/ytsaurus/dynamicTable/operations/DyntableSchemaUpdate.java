package ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.TableRow;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.DyntableUtils;
import ru.dzen.kafka.connect.ytsaurus.schema.SchemaUtils;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.AlterTable;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;

/**
 * @author pbk-vitaliy
 */
public class DyntableSchemaUpdate implements TableOperation {
  private static final Logger log = LoggerFactory.getLogger(DyntableCreate.class);

  private final YPath tablePath;
  private final TableRow rowWithUpdatedSchema;
  private TableSchema currentSchema;

  public DyntableSchemaUpdate(
      YPath tablePath,
      TableSchema currentSchema,
      TableRow rowWithUpdatedSchema) {
    this.tablePath = tablePath;
    this.rowWithUpdatedSchema = rowWithUpdatedSchema;
    this.currentSchema = currentSchema;
  }

  @Override
  public void execute(ApiServiceTransaction tx) {
    TableSchema currentSchema = getCurrentSchema();
    TableSchema updatedSchema = rowWithUpdatedSchema.getSchema();
    TableSchema newSchema = SchemaUtils.mergeSchemas(currentSchema, updatedSchema);
    if (alteringRequired(currentSchema, newSchema)) {
      alterTableSchema(tx, newSchema);
    }
  }

  public TableSchema getCurrentSchema() {
    return currentSchema;
  }

  private boolean alteringRequired(TableSchema currentSchema, TableSchema newSchema) {
    return currentSchema.getColumns().size() != newSchema.getColumns().size();
  }

  private void alterTableSchema(ApiServiceTransaction tx, TableSchema schema) {
    DyntableUtils.freezeTable(tx.getClient(), tablePath);
    DyntableUtils.unmountTable(tx.getClient(), tablePath);
    tx.getClient().alterTable(AlterTable.builder()
        .setPath(tablePath)
        .setSchema(schema)
        .build()
    ).join();
    log.info("Updated table {} schema to {}", tablePath, schema);
    DyntableUtils.mountTable(tx.getClient(), tablePath);
    this.currentSchema = schema;
  }
}

package ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations;

import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * @author pbk-vitaliy
 */
public class TableSchemaRead implements TableOperation {

  private final YPath tablePath;
  private TableSchema schema;

  public TableSchemaRead(YPath tablePath) {
    this.tablePath = tablePath;
  }

  @Override
  public void execute(ApiServiceTransaction tx) {
    YTreeNode schemaAttr = tx.getNode(tablePath + "/@schema").join();
    schema = TableSchema.fromYTree(schemaAttr);
  }

  public TableSchema getSchema() {
    return schema;
  }
}

package ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.DynTableWriterConfig.TableType;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * @author pbk-vitaliy
 */
public class DyntableCreate implements TableOperation {
  private static final Logger log = LoggerFactory.getLogger(DyntableCreate.class);

  private final TableType tableType;
  private final YPath tablePath;
  private final TableSchema tableSchema;
  private final Map<String, YTreeNode> tableAttributes;

  public DyntableCreate(
      TableType tableType,
      YPath tablePath,
      TableSchema tableSchema,
      Map<String, YTreeNode> tableAttributes) {
    this.tableType = tableType;
    this.tablePath = tablePath;
    this.tableSchema = tableSchema;
    this.tableAttributes = tableAttributes;
  }

  @Override
  public void execute(ApiServiceTransaction tx) {
    TableSchema schema = tableSchema;
    if (tableType == TableType.ORDERED) {
      schema = transformKeyColumns(schema);
    }
    Map<String, YTreeNode> attributes = new HashMap<>();
    attributes.put("dynamic", YTree.booleanNode(true));
    attributes.put("schema", schema.toYTree());
    attributes.put("enable_dynamic_store_read", YTree.booleanNode(true));
    attributes.putAll(tableAttributes);
    var createNodeRq = CreateNode.builder()
        .setPath(tablePath)
        .setType(CypressNodeType.TABLE)
        .setAttributes(attributes)
        .setIgnoreExisting(true)
        .build();
    tx.createNode(createNodeRq).join();
    log.info("Created table {}", schema);
  }

  private TableSchema transformKeyColumns(TableSchema schema) {
    List<ColumnSchema> columns = schema.getColumns().stream()
        .map(col -> col.toBuilder().setSortOrder(null).build())
        .collect(Collectors.toList());
    return schema.toBuilder()
        .setColumns(columns)
        .setUniqueKeys(false)
        .build();
  }
}

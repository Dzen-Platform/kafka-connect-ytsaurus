package ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * @author pbk-vitaliy
 */
public class DyntableCreate implements TableOperation {
  private static final Logger log = LoggerFactory.getLogger(DyntableCreate.class);

  private final YPath tablePath;
  private final TableSchema tableSchema;
  private final Map<String, YTreeNode> tableAttributes;

  public DyntableCreate(
      YPath tablePath,
      TableSchema tableSchema,
      Map<String, YTreeNode> tableAttributes) {
    this.tablePath = tablePath;
    this.tableSchema = tableSchema;
    this.tableAttributes = tableAttributes;
  }

  @Override
  public void execute(ApiServiceTransaction tx) {
    Map<String, YTreeNode> attributes = new HashMap<>();
    attributes.put("dynamic", YTree.booleanNode(true));
    attributes.put("schema", tableSchema.toYTree());
    attributes.put("enable_dynamic_store_read", YTree.booleanNode(true));
    attributes.putAll(tableAttributes);
    var createNodeRq = CreateNode.builder()
        .setPath(tablePath)
        .setType(CypressNodeType.TABLE)
        .setAttributes(attributes)
        .setIgnoreExisting(true)
        .build();
    tx.createNode(createNodeRq).join();
    log.info("Created table {}", tableSchema);
  }
}

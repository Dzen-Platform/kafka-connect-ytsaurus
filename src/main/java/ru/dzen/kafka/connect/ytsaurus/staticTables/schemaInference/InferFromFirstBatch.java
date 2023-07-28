package ru.dzen.kafka.connect.ytsaurus.staticTables.schemaInference;

import java.util.Collection;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.TableRow;
import ru.dzen.kafka.connect.ytsaurus.schema.SchemaUtils;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * @author pbk-vitaliy
 */
public class InferFromFirstBatch implements SchemaInferenceStrategyImpl {
  private static final Logger log = LoggerFactory.getLogger(InferFromFirstBatch.class);

  private TableSchema currentSchema = null;

  @Override
  public void init(ApiServiceTransaction tx, YPath tablePath) {
    if (!tx.existsNode(tablePath.attribute("schema").toString()).join()) {
      return;
    }
    GetNode schemaReq = GetNode.builder()
        .setPath(tablePath.attribute("schema"))
        .build();
    YTreeNode schemaNode = tx.getNode(schemaReq).join();
    if (schemaNode != null && schemaNode.isListNode()) {
      currentSchema = TableSchema.fromYTree(schemaNode);
      log.trace("Loaded schema {} from existing table {}", currentSchema, tablePath);
    }
  }

  @Override
  public void update(ApiServiceTransaction tx, Collection<TableRow> tableRows) {
    if (currentSchema == null) {
      for (TableRow r : tableRows) {
        currentSchema = SchemaUtils.mergeSchemas(currentSchema, r.getSchema());
      }
    }
  }

  @Override
  public Optional<TableSchema> getSchema(ApiServiceTransaction tx) {
    return Optional.ofNullable(currentSchema);
  }
}

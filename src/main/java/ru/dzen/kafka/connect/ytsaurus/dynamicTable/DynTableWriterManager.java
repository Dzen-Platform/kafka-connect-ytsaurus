package ru.dzen.kafka.connect.ytsaurus.dynamicTable;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.TableWriterManager;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

public class DynTableWriterManager extends DynTableWriter implements TableWriterManager {

  private static final Logger log = LoggerFactory.getLogger(DynTableWriterManager.class);

  public DynTableWriterManager(DynTableWriterConfig config) {
    super(config);
  }

  protected void createDynamicTable(YTsaurusClient client, YPath path, TableSchema schema,
      Map<String, YTreeNode> extraAttributes)
      throws Exception {
    Map<String, YTreeNode> attributes = new HashMap<>(Map.of(
        "dynamic", YTree.booleanNode(true),
        "schema", schema.toYTree(),
        "enable_dynamic_store_read", YTree.booleanNode(true)
    ));
    attributes.putAll(extraAttributes);
    var createNodeBuilder = CreateNode.builder().setPath(path)
        .setType(CypressNodeType.TABLE).setAttributes(attributes).setIgnoreExisting(true);
    client.createNode(createNodeBuilder.build()).get();
    log.info("Created table {}", path);
  }

  @Override
  public void start() throws RetriableException {
    if (!config.getAutoCreateTables()) {
      return;
    }
    try {
      var createNodeBuilder = CreateNode.builder()
          .setPath(config.getMetadataDirectory())
          .setType(CypressNodeType.MAP)
          .setRecursive(true)
          .setIgnoreExisting(true);
      client.createNode(createNodeBuilder.build()).get();
    } catch (Exception e) {
      throw new RetriableException(e);
    }
    try {
      createDynamicTable(client, config.getOffsetsTablePath(),
          UnstructuredTableSchema.OFFSETS_TABLE_SCHEMA, config.getTableExtraAttributes());
      DyntableUtils.mountTable(client, config.getOffsetsTablePath());
    } catch (Exception ex) {
      log.warn("Cannot initialize queue", ex);
      throw new RetriableException(ex);
    }
  }

  @Override
  public void stop() {

  }
}

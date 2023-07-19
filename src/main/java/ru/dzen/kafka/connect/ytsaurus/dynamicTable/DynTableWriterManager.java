package ru.dzen.kafka.connect.ytsaurus.dynamicTable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.TableWriterManager;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema.EColumn;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema.ETableType;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.DynTableWriterConfig.UpdateMode;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.ReshardTable;
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
    var attributes = new HashMap<>(Map.of(
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

  protected void mountDynamicTable(YTsaurusClient client, YPath path) throws Exception {
    log.info("Trying to mount table {}", path);
    client.mountTableAndWaitTablets(new MountTable(path)).get();
    log.info("Mounted table {}", path);
  }

  protected void reshardQueueAndSetAttributesIfNeeded(YTsaurusClient client) throws Exception {
    var attributes = client.getNode(config.getDataQueueTablePath() + "/@").get().mapNode();
    for (var entry : config.getExtraQueueAttributes().entrySet()) {
      if (!Objects.equals(attributes.get(entry.getKey()), Optional.of(entry.getValue()))) {
        client.setNode(config.getDataQueueTablePath() + "/@" + entry.getKey(), entry.getValue());
        log.info(
            "Changed {}/@{} to {}", config.getDataQueueTablePath(), entry.getKey(),
            entry.getValue());
      }
    }
    var currentTabletCount = attributes.get("tablet_count").map(YTreeNode::intValue);
    if (currentTabletCount.equals(Optional.of(config.getTabletCount()))) {
      log.info("No need to reshard table {}: tablet count = desired tablet count",
          config.getDataQueueTablePath());
      return;
    }

    log.info("Unmounting table {}", config.getDataQueueTablePath());
    client.unmountTableAndWaitTablets(config.getDataQueueTablePath().toString()).join();
    log.info("Unmounted table {}", config.getDataQueueTablePath());

    log.info("Resharding table {}", config.getDataQueueTablePath());
    client.reshardTable(ReshardTable.builder().setPath(config.getDataQueueTablePath())
        .setTabletCount(config.getTabletCount()).build()).get();
    log.info("Successfully resharded table {}", config.getDataQueueTablePath());
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
      var dataQueueTableSchema = getDyntableSchema();

      var desiredExtraAttributesWithTabletCount = new HashMap<>(config.getExtraQueueAttributes());
      desiredExtraAttributesWithTabletCount.put("tablet_count",
          YTree.node(config.getTabletCount()));
      createDynamicTable(client, config.getDataQueueTablePath(), dataQueueTableSchema,
          desiredExtraAttributesWithTabletCount);

      reshardQueueAndSetAttributesIfNeeded(client);

      mountDynamicTable(client, config.getDataQueueTablePath());
      createDynamicTable(client, config.getOffsetsTablePath(),
          UnstructuredTableSchema.OFFSETS_TABLE_SCHEMA, config.getExtraQueueAttributes());
      mountDynamicTable(client, config.getOffsetsTablePath());
    } catch (Exception ex) {
      log.warn("Cannot initialize queue", ex);
      throw new RetriableException(ex);
    }
  }

  @Override
  public void stop() {

  }

  private TableSchema getDyntableSchema() {
    return config.getUpdateMode() == UpdateMode.CRUD
        ? UnstructuredTableSchema.createSortedTableSchema(
            config.getKeyOutputFormat(),
            config.getValueOutputFormat(),
            EColumn.getAllMetadataColumns(ETableType.DYNAMIC))
        : UnstructuredTableSchema.createDataQueueTableSchema(
            config.getKeyOutputFormat(),
            config.getValueOutputFormat(),
            EColumn.getAllMetadataColumns(ETableType.DYNAMIC));
  }
}

package ru.dzen.kafka.connect.ytsaurus.dynamicTable;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

public class DynTableWriterConfig extends BaseTableWriterConfig {

  public static final String QUEUE_POSTFIX = "yt.sink.dynamic.queue.postfix";
  public static final String QUEUE_AUTO_CREATE = "yt.sink.dynamic.queue.auto.create";
  public static final String QUEUE_TABLET_COUNT = "yt.sink.dynamic.queue.tablet.count";
  public static final String OPERATION_FIELD = "yt.sink.dynamic.operation.field";
  public static final String TABLE_ROUTER_ENABLED = "yt.sink.dynamic.table.router.enabled";
  public static final String TABLE_ROUTER_FIELD = "yt.sink.dynamic.table.router.field";
  public static ConfigDef CONFIG_DEF = new ConfigDef(BaseTableWriterConfig.CONFIG_DEF)
      .define(QUEUE_POSTFIX, ConfigDef.Type.STRING, "queue", ConfigDef.Importance.MEDIUM,
          "Postfix for the data queue table name in dynamic output mode")
      .define(QUEUE_AUTO_CREATE, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
          "Flag to automatically create and mount dynamic tables if they do not exist")
      .define(QUEUE_TABLET_COUNT, ConfigDef.Type.INT, 1, ConfigDef.Range.atLeast(1),
          ConfigDef.Importance.MEDIUM,
          "Number of tablets for the data queue table in dynamic output mode")
      .define(OPERATION_FIELD, ConfigDef.Type.STRING, "_op", ConfigDef.Importance.HIGH,
          "Which field name to use with ExtractField transformer to extract operation type. Works only with TABLE_TYPE 'SORTED'. Defaults to 'UPDATE'")
      .define(TABLE_ROUTER_ENABLED, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH,
          "Flag to enable records routing from single topic to different dynamic tables")
      .define(TABLE_ROUTER_FIELD, ConfigDef.Type.STRING, "_tbl", ConfigDef.Importance.HIGH,
          "Which field name to use with ExtractField transformer to extract table name.");

  public DynTableWriterConfig(Map<String, String> originals) throws ConnectException {
    super(CONFIG_DEF, originals);
  }

  public YPath getDataQueueTablePath() {
    return getOutputDirectory().child(getString(QUEUE_POSTFIX));
  }

  public YPath getOffsetsTablePath() {
    return getMetadataDirectory().child("offsets");
  }

  public boolean getAutoCreateTables() {
    return getBoolean(QUEUE_AUTO_CREATE);
  }

  public int getTabletCount() {
    return getInt(QUEUE_TABLET_COUNT);
  }

  public String getOperationField() {
    return getString(OPERATION_FIELD);
  }

  public boolean isTableRouterEnabled() {
    return getBoolean(TABLE_ROUTER_ENABLED);
  }

  public Map<String, YTreeNode> getTableExtraAttributes() {
    return Map.of(
        "min_data_versions", YTree.integerNode(0),
        "max_data_versions", YTree.integerNode(1),
        "min_data_ttl", YTree.longNode(0),
        "max_data_ttl", YTree.longNode(getOutputTTL().toMillis()));
  }
}

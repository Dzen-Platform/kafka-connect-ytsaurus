package ru.dzen.kafka.connect.ytsaurus.dynamicTable;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

public class DynTableWriterConfig extends BaseTableWriterConfig {

  private static final String QUEUE_POSTFIX = "yt.sink.dynamic.queue.postfix";
  private static final String QUEUE_AUTO_CREATE = "yt.sink.dynamic.queue.auto.create";
  private static final String QUEUE_TABLET_COUNT = "yt.sink.dynamic.queue.tablet.count";
  private static final String UPDATE_MODE = "yt.sink.dynamic.update.mode";
  public static ConfigDef CONFIG_DEF = new ConfigDef(BaseTableWriterConfig.CONFIG_DEF)
      .define(QUEUE_POSTFIX, ConfigDef.Type.STRING, "queue", ConfigDef.Importance.MEDIUM,
          "Postfix for the data queue table name in dynamic output mode")
      .define(QUEUE_AUTO_CREATE, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
          "Flag to automatically create and mount dynamic tables if they do not exist")
      .define(QUEUE_TABLET_COUNT, ConfigDef.Type.INT, 1, ConfigDef.Range.atLeast(1),
          ConfigDef.Importance.MEDIUM,
          "Number of tablets for the data queue table in dynamic output mode")
      .define(UPDATE_MODE, ConfigDef.Type.STRING, "inserts", ConfigDef.Importance.MEDIUM,
          "Interpret all sink records as inserts, or try to perform other operations");

  public DynTableWriterConfig(Map<String, String> originals) throws ConnectException {
    super(CONFIG_DEF, originals);

    if (!getOutputTableSchemaType().equals(OutputTableSchemaType.UNSTRUCTURED)) {
      throw new ConnectException("Only UNSTRUCTURED schema type is supported by DynTableWriter!");
    }
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

  public UpdateMode getUpdateMode() {
    return UpdateMode.valueOf(getString(UPDATE_MODE).toUpperCase());
  }

  public Map<String, YTreeNode> getExtraQueueAttributes() {
    return Map.of(
        "min_data_versions", YTree.integerNode(0),
        "max_data_versions", YTree.integerNode(1),
        "min_data_ttl", YTree.longNode(0),
        "max_data_ttl", YTree.longNode(getOutputTTL().toMillis()));
  }

  public enum UpdateMode {
    INSERTS,
  }
}

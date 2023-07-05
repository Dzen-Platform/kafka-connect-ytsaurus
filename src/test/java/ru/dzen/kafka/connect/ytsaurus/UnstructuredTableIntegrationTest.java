package ru.dzen.kafka.connect.ytsaurus;

import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputFormat;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputTableSchemaType;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputType;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema.EColumn;
import ru.dzen.kafka.connect.ytsaurus.integration.BaseYtsaurusConnectorIntegrationTest;
import ru.dzen.kafka.connect.ytsaurus.staticTables.StaticTableWriterConfig;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * @author pbk-vitaliy
 */
public class UnstructuredTableIntegrationTest extends BaseYtsaurusConnectorIntegrationTest {

  private static final String topic = "test-unstructured";
  private static final String ytsaurusOutputDirectory =
      String.format("//home/%s/test", topic);
  private static final String offsetsPath =
      String.format("%s/__connect_sink_metadata__/offsets/%s-0", ytsaurusOutputDirectory, topic);
  private static final String outputTableDir =
      String.format("%s/output", ytsaurusOutputDirectory);
  private static final String key = "123";
  private static final String value = "text";
  private static final String keyJson = String.format("{\"id\":\"%s\"}", key);
  private static final String valueJson = String.format("{\"value\":\"%s\"}", value);

  @BeforeEach
  void setUp() {
    this.sinkConnectorProps = baseSinkConnectorProps();
    sinkConnectorProps.put("value.converter.schemas.enable", "false");
    sinkConnectorProps.put(SinkConnectorConfig.TOPICS_CONFIG, topic);
    sinkConnectorProps.put(BaseTableWriterConfig.OUTPUT_TYPE, OutputType.STATIC_TABLES.name());
    sinkConnectorProps.put(BaseTableWriterConfig.OUTPUT_DIRECTORY, ytsaurusOutputDirectory);
    sinkConnectorProps.put(BaseTableWriterConfig.OUTPUT_TABLE_SCHEMA_TYPE, OutputTableSchemaType.UNSTRUCTURED.name());
    sinkConnectorProps.put(BaseTableWriterConfig.KEY_OUTPUT_FORMAT, OutputFormat.STRING.name());
    sinkConnectorProps.put(BaseTableWriterConfig.VALUE_OUTPUT_FORMAT, OutputFormat.STRING.name());
    sinkConnectorProps.put(StaticTableWriterConfig.ROTATION_PERIOD, "5m");
    startConnect();
    connect.kafka().createTopic(topic);
  }

  @AfterEach
  void tearDown() {
    stopConnect();
  }

  @Test
  void writesMessagesToUnstructuredTable() {
    String connectorName = "ytsaurus-connector-unstructured";
    connect.configureConnector(connectorName, sinkConnectorProps);
    awaitConnectorIsStarted(connectorName);
    int totalMessages = 10;

    for (int i = 0; i < totalMessages; i++) {
      connect.kafka().produce(topic, keyJson, valueJson);
    }
    awaitCommittedOffset(connectorName, topic, 0, totalMessages - 1);

    assertCypress().node(offsetsPath)
        .valueEquals(YTree.mapBuilder().key("offset").value(totalMessages - 1).buildMap());
    assertTable().anyStaticTableInDir(outputTableDir).exists();
    assertTable().anyStaticTableInDir(outputTableDir).rows()
        .hasSize(totalMessages)
        .allMatch(row -> hasColumnContaining(row, EColumn.KEY.name, key))
        .allMatch(row -> hasColumnContaining(row, EColumn.DATA.name, value))
        .are(hasDifferentOffsets());
  }

  private boolean hasColumnContaining(YTreeMapNode row, String columnName, String textToSearch) {
    return row.containsKey(columnName)
        && row.get(columnName)
        .filter(YTreeNode::isStringNode)
        .map(v -> v.stringNode().stringValue().contains(textToSearch))
        .orElse(false);
  }

  private Condition<? super YTreeMapNode> hasDifferentOffsets() {
    Set<Integer> encounteredOffsets = new HashSet<>();
    return new Condition<>(row -> {
      int offset = row.getInt(EColumn.OFFSET.name);
      return encounteredOffsets.add(offset);
    }, "Different offsets in table");
  }
}

package ru.dzen.kafka.connect.ytsaurus;

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

/**
 * @author pbk-vitaliy
 */
public class DynamicTableInsertIntegrationTest extends BaseYtsaurusConnectorIntegrationTest {

  private static final String topic = "test-dynamic-insert";
  private static final String ytsaurusOutputDirectory =
      String.format("//home/%s/test", topic);
  private static final String offsetsPath =
      String.format("%s/__connect_sink_metadata__/offsets", ytsaurusOutputDirectory);
  private static final String outputTablePath =
      String.format("%s/queue", ytsaurusOutputDirectory);
  private static final String key = "123";
  private static final String value = "text";
  private static final String keyJson = String.format("{\"id\":\"%s\"}", key);
  private static final String valueJson = String.format("{\"value\":\"%s\"}", value);

  @BeforeEach
  void setUp() {
    this.sinkConnectorProps = baseSinkConnectorProps();
    sinkConnectorProps.put("value.converter.schemas.enable", "false");
    sinkConnectorProps.put(SinkConnectorConfig.TOPICS_CONFIG, topic);
    sinkConnectorProps.put(BaseTableWriterConfig.OUTPUT_TYPE, OutputType.DYNAMIC_TABLE.name());
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
  void writesMessagesToDynamicTable() {
    String connectorName = "ytsaurus-connector-dynamic";
    connect.configureConnector(connectorName, sinkConnectorProps);
    awaitConnectorIsStarted(connectorName);
    int totalMessages = 10;
    int expectedOffset = totalMessages - 1;

    for (int i = 0; i < totalMessages; i++) {
      connect.kafka().produce(topic, keyJson, valueJson);
    }
    awaitCommittedOffset(connectorName, topic, 0, expectedOffset);

    assertTable().dynamicTableWithPath(offsetsPath).rows()
        .hasSize(1)
        .allMatch(row -> row.getInt(EColumn.OFFSET.name) == expectedOffset);
    assertTable().dynamicTableWithPath(outputTablePath).rows()
        .hasSize(totalMessages)
        .allMatch(RowPredicates.hasColumnContaining(EColumn.KEY.name, key))
        .allMatch(RowPredicates.hasColumnContaining(EColumn.DATA.name, value))
        .are(new Condition<>(RowPredicates.hasDifferentIntValues(EColumn.OFFSET.name), "Different offsets in rows"));
  }
}

package ru.dzen.kafka.connect.ytsaurus;

import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputFormat;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputTableSchemaType;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputType;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema.EColumn;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.DynTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.DynTableWriterConfig.UpdateMode;
import ru.dzen.kafka.connect.ytsaurus.integration.BaseYtsaurusConnectorIntegrationTest;
import ru.dzen.kafka.connect.ytsaurus.staticTables.StaticTableWriterConfig;

/**
 * @author pbk-vitaliy
 */
public class DynamicTableCrudIntegrationTest extends BaseYtsaurusConnectorIntegrationTest {

  private static final String topic = "test-dynamic-crud";
  private static final String ytsaurusOutputDirectory =
      String.format("//home/%s/test", topic);
  private static final String offsetsPath =
      String.format("%s/__connect_sink_metadata__/offsets", ytsaurusOutputDirectory);
  private static final String outputTablePath =
      String.format("%s/queue", ytsaurusOutputDirectory);

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
    sinkConnectorProps.put(DynTableWriterConfig.UPDATE_MODE, UpdateMode.CRUD.name().toLowerCase());
    sinkConnectorProps.put(DynTableWriterConfig.OPERATION_FIELD, "_op");
    startConnect();
    connect.kafka().createTopic(topic);
  }

  @AfterEach
  void tearDown() {
    stopConnect();
  }

  @Test
  void deletesRowsInCrudMode() {
    String connectorName = "ytsaurus-connector-dynamic-crud";
    connect.configureConnector(connectorName, sinkConnectorProps);
    awaitConnectorIsStarted(connectorName);

    String key1Json = "{\"id\":\"111\"}";
    String key2Json = "{\"id\":\"222\"}";
    String createValueJson = "{\"value\":\"text\",\"_op\":\"c\"}";
    String deleteValueJson = "{\"value\":null,\"_op\":\"d\"}";
    connect.kafka().produce(topic, key1Json, createValueJson);
    connect.kafka().produce(topic, key2Json, createValueJson);

    awaitCommittedOffset(connectorName, topic, 0, 1);

    assertTable().dynamicTableWithPath(offsetsPath).rows()
        .hasSize(1)
        .allMatch(row -> row.getInt(EColumn.OFFSET.name) == 1);
    assertTable().dynamicTableWithPath(outputTablePath).rows()
        .hasSize(2);

    connect.kafka().produce(topic, key2Json, deleteValueJson);

    awaitCommittedOffset(connectorName, topic, 0, 2);
    assertTable().dynamicTableWithPath(offsetsPath).rows()
        .allMatch(row -> row.getInt(EColumn.OFFSET.name) == 2);
    assertTable().dynamicTableWithPath(outputTablePath).rows()
        .hasSize(1)
        .noneMatch(RowPredicates.hasColumnContaining(EColumn.KEY.name, key2Json));
  }
}

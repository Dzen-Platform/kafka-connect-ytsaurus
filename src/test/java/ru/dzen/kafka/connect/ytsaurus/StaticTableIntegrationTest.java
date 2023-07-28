package ru.dzen.kafka.connect.ytsaurus;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.assertj.core.api.Condition;
import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputFormat;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputType;
import ru.dzen.kafka.connect.ytsaurus.common.FlattenedSchemaTableRowMapper;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredSchemaTableRowMapper;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema.EColumn;
import ru.dzen.kafka.connect.ytsaurus.integration.BaseYtsaurusConnectorIntegrationTest;
import ru.dzen.kafka.connect.ytsaurus.staticTables.StaticTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.staticTables.StaticTableWriterConfig.SchemaInferenceStrategy;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

/**
 * @author pbk-vitaliy
 */
public class StaticTableIntegrationTest extends BaseYtsaurusConnectorIntegrationTest {

  @BeforeEach
  void setUp() {
    startConnect();
  }

  @AfterEach
  void tearDown() {
    stopConnect();
  }

  @Test
  void unstructuredWeak() {
    String connectorName = "ytsaurus-unstructured-weak";
    connect.kafka().createTopic(connectorName);
    connect.configureConnector(connectorName, getConnectorProps(connectorName));
    awaitConnectorIsStarted(connectorName);
    List<Message> testMessages = getTestMessages(connectorName);
    int expectedOffset = testMessages.size() - 1;

    testMessages.forEach(m -> connect.kafka().produce(connectorName, m.key, m.value));
    awaitCommittedOffset(connectorName, connectorName, 0, expectedOffset);

    assertCypress().node(getOffsetsPath(connectorName)).is()
        .isEqualTo(YTree.mapBuilder().key("offset").value(expectedOffset).buildMap());
    assertTable().anyStaticTableInDir(getOutputTableDir(connectorName)).exists();
    ListAssert<YTreeMapNode> assertions = assertTable()
        .anyStaticTableInDir(getOutputTableDir(connectorName))
        .rows()
        .hasSize(testMessages.size())
        .are(new Condition<>(RowPredicates.hasDifferentIntValues(EColumn.OFFSET.name),
            "Different offsets in rows"));
    for (Message m : testMessages) {
      assertions.anyMatch(RowPredicates.hasColumnContaining(EColumn.KEY.name, m.key));
      assertions.anyMatch(RowPredicates.hasColumnContaining(EColumn.DATA.name, m.value));
    }
  }

  @Test
  void flattenedWeak() {
    String connectorName = "ytsaurus-flattened-weak";
    connect.kafka().createTopic(connectorName);
    connect.configureConnector(connectorName, getConnectorProps(connectorName));
    awaitConnectorIsStarted(connectorName);
    List<Message> testMessages = getTestMessages(connectorName);
    int expectedOffset = testMessages.size() - 1;

    testMessages.forEach(m -> connect.kafka().produce(connectorName, m.key, m.value));
    awaitCommittedOffset(connectorName, connectorName, 0, expectedOffset);

    assertCypress().node(getOffsetsPath(connectorName)).is()
        .isEqualTo(YTree.mapBuilder().key("offset").value(expectedOffset).buildMap());
    assertTable().anyStaticTableInDir(getOutputTableDir(connectorName)).exists();
    ListAssert<YTreeMapNode> assertions = assertTable()
        .anyStaticTableInDir(getOutputTableDir(connectorName))
        .rows()
        .hasSize(testMessages.size())
        .are(new Condition<>(RowPredicates.hasDifferentIntValues(EColumn.OFFSET.name),
            "Different offsets in rows"));
    for (int i = 0; i < testMessages.size(); i++) {
      assertions.anyMatch(RowPredicates.hasColumnContaining("id_" + i, String.valueOf(i)));
      assertions.anyMatch(RowPredicates.hasColumnContaining("value_" + i, String.format("value_%d", i)));
    }
  }

  @Test
  void unstructuredStrictFirstBatchInference() {
    String connectorName = "ytsaurus-unstructured-strict-fb";
    connect.kafka().createTopic(connectorName);
    connect.configureConnector(connectorName, getConnectorProps(connectorName));
    awaitConnectorIsStarted(connectorName);
    List<Message> testMessages = getTestMessages(connectorName);
    int expectedOffset = testMessages.size() - 1;

    testMessages.forEach(m -> connect.kafka().produce(connectorName, m.key, m.value));
    awaitCommittedOffset(connectorName, connectorName, 0, expectedOffset);

    assertCypress().node(getOffsetsPath(connectorName)).is()
        .isEqualTo(YTree.mapBuilder().key("offset").value(expectedOffset).buildMap());
    assertTable().anyStaticTableInDir(getOutputTableDir(connectorName)).exists();
    ListAssert<YTreeMapNode> assertions = assertTable()
        .anyStaticTableInDir(getOutputTableDir(connectorName))
        .rows()
        .hasSize(testMessages.size())
        .are(new Condition<>(RowPredicates.hasDifferentIntValues(EColumn.OFFSET.name),
            "Different offsets in rows"));
    for (Message m : testMessages) {
      assertions.anyMatch(RowPredicates.hasColumnContaining(EColumn.KEY.name, m.key));
      assertions.anyMatch(RowPredicates.hasColumnContaining(EColumn.DATA.name, m.value));
    }
  }

  @Test
  void flattenedStrictFirstBatchInference() {
    String connectorName = "ytsaurus-flattened-strict-fb";
    connect.kafka().createTopic(connectorName);
    connect.configureConnector(connectorName, getConnectorProps(connectorName));
    awaitConnectorIsStarted(connectorName);
    List<Message> testMessages = getTestMessages(connectorName);
    int expectedOffset = testMessages.size() - 1;

    testMessages.forEach(m -> connect.kafka().produce(connectorName, m.key, m.value));
    awaitCommittedOffset(connectorName, connectorName, 0, expectedOffset);

    assertCypress().node(getOffsetsPath(connectorName)).is()
        .isEqualTo(YTree.mapBuilder().key("offset").value(expectedOffset).buildMap());
    assertTable().anyStaticTableInDir(getOutputTableDir(connectorName)).exists();
    assertTable().anyStaticTableInDir(getOutputTableDir(connectorName)).rows()
        .hasSize(testMessages.size())
        .anyMatch(RowPredicates.hasColumnContaining("id_0", "0"))
        .anyMatch(RowPredicates.hasColumnContaining("value_0", "value_0"))
        .are(new Condition<>(RowPredicates.hasDifferentIntValues(EColumn.OFFSET.name),
            "Different offsets in rows"));
  }

  private Map<String, String> getConnectorProps(String connectorName) {
    Map<String, String> props = baseSinkConnectorProps();
    props.put("value.converter.schemas.enable", "false");
    props.put(SinkConnectorConfig.TOPICS_CONFIG, connectorName);
    props.put(BaseTableWriterConfig.OUTPUT_TYPE, OutputType.STATIC_TABLES.name());
    props.put(BaseTableWriterConfig.OUTPUT_DIRECTORY, getOutputDir(connectorName));
    props.put(StaticTableWriterConfig.ROTATION_PERIOD, "5m");

    switch (connectorName) {
      case "ytsaurus-unstructured-weak" -> {
        props.put(BaseTableWriterConfig.ROW_MAPPER_CLASS,
            UnstructuredSchemaTableRowMapper.class.getName());
        props.put(StaticTableWriterConfig.SCHEMA_INFERENCE_STRATEGY,
            SchemaInferenceStrategy.DISABLED.name());
        props.put(BaseTableWriterConfig.KEY_OUTPUT_FORMAT, OutputFormat.STRING.name());
        props.put(BaseTableWriterConfig.VALUE_OUTPUT_FORMAT, OutputFormat.STRING.name());
      }
      case "ytsaurus-flattened-weak" -> {
        props.put(BaseTableWriterConfig.ROW_MAPPER_CLASS,
            FlattenedSchemaTableRowMapper.class.getName());
        props.put(StaticTableWriterConfig.SCHEMA_INFERENCE_STRATEGY,
            SchemaInferenceStrategy.DISABLED.name());
      }
      case "ytsaurus-unstructured-strict-fb" -> {
        props.put(BaseTableWriterConfig.ROW_MAPPER_CLASS,
            UnstructuredSchemaTableRowMapper.class.getName());
        props.put(StaticTableWriterConfig.SCHEMA_INFERENCE_STRATEGY,
            SchemaInferenceStrategy.INFER_FROM_FIRST_BATCH.name());
        props.put(BaseTableWriterConfig.KEY_OUTPUT_FORMAT, OutputFormat.STRING.name());
        props.put(BaseTableWriterConfig.VALUE_OUTPUT_FORMAT, OutputFormat.STRING.name());
      }
      case "ytsaurus-flattened-strict-fb" -> {
        props.put(BaseTableWriterConfig.ROW_MAPPER_CLASS,
            FlattenedSchemaTableRowMapper.class.getName());
        props.put(StaticTableWriterConfig.SCHEMA_INFERENCE_STRATEGY,
            SchemaInferenceStrategy.INFER_FROM_FIRST_BATCH.name());
      }
    }

    return props;
  }

  private List<Message> getTestMessages(String connectorName) {
    return switch (connectorName) {
      case "ytsaurus-unstructured-weak", "ytsaurus-unstructured-strict-fb" -> IntStream.range(0, 10)
          .mapToObj(i -> new Message(
              String.format("{\"id\":\"%d\"}", i),
              String.format("{\"value\":\"value_%d\"}", i)))
          .collect(Collectors.toList());
      case "ytsaurus-flattened-weak", "ytsaurus-flattened-strict-fb" -> IntStream.range(0, 10)
          .mapToObj(i -> new Message(
              String.format("{\"id_%d\":\"%d\"}", i, i),
              String.format("{\"value_%d\":\"value_%d\"}", i, i)))
          .collect(Collectors.toList());
      default -> List.of();
    };
  }

  private static String getOutputDir(String connectorName) {
    return String.format("//home/%s/test", connectorName);
  }

  private static String getOffsetsPath(String connectorName) {
    return String.format("%s/__connect_sink_metadata__/offsets/0", getOutputDir(connectorName));
  }

  private static String getOutputTableDir(String connectorName) {
    return String.format("%s/output", getOutputDir(connectorName));
  }


  private final class Message {
    private final String key;
    private final String value;

    public Message(String key, String value) {
      this.key = key;
      this.value = value;
    }
  }
}

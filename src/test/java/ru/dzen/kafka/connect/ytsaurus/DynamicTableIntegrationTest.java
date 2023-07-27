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
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.DynTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.DynTableWriterConfig.TableType;
import ru.dzen.kafka.connect.ytsaurus.integration.BaseYtsaurusConnectorIntegrationTest;
import ru.dzen.kafka.connect.ytsaurus.staticTables.StaticTableWriterConfig;
import tech.ytsaurus.ysontree.YTreeMapNode;

/**
 * @author pbk-vitaliy
 */
public class DynamicTableIntegrationTest extends BaseYtsaurusConnectorIntegrationTest {

  @BeforeEach
  void setUp() {
    startConnect();
  }

  @AfterEach
  void tearDown() {
    stopConnect();
  }

  @Test
  void orderedUnstructured() {
    String connectorName = "ytsaurus-ordered-unstructured";
    connect.kafka().createTopic(connectorName);
    connect.configureConnector(connectorName, getConnectorProps(connectorName));
    awaitConnectorIsStarted(connectorName);
    List<Message> testMessages = getTestMessages(connectorName);
    int expectedOffset = testMessages.size() - 1;

    testMessages.forEach(m -> connect.kafka().produce(connectorName, m.key, m.value));
    awaitCommittedOffset(connectorName, connectorName, 0, expectedOffset);

    assertTable().dynamicTableWithPath(getOffsetsTablePath(connectorName)).rows()
        .hasSize(1)
        .allMatch(row -> row.getInt(EColumn.OFFSET.name) == expectedOffset);
    ListAssert<YTreeMapNode> assertions = assertTable()
        .dynamicTableWithPath(getOutputTablePath(connectorName))
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
  void orderedFlattened() {
    String connectorName = "ytsaurus-ordered-flattened";
    connect.kafka().createTopic(connectorName);
    connect.configureConnector(connectorName, getConnectorProps(connectorName));
    awaitConnectorIsStarted(connectorName);
    List<Message> testMessages = getTestMessages(connectorName);
    int expectedOffset = testMessages.size() - 1;

    testMessages.forEach(m -> connect.kafka().produce(connectorName, m.key, m.value));
    awaitCommittedOffset(connectorName, connectorName, 0, expectedOffset);

    assertTable().dynamicTableWithPath(getOffsetsTablePath(connectorName)).rows()
        .hasSize(1)
        .allMatch(row -> row.getInt(EColumn.OFFSET.name) == expectedOffset);
    assertTable().dynamicTableWithPath(getOutputTablePath(connectorName)).rows()
        .hasSize(testMessages.size())
        .are(new Condition<>(RowPredicates.hasDifferentIntValues(EColumn.OFFSET.name),
            "Different offsets in rows"));
  }

  @Test
  void sortedUnstructured() {
    String connectorName = "ytsaurus-sorted-unstructured";
    connect.kafka().createTopic(connectorName);
    connect.configureConnector(connectorName, getConnectorProps(connectorName));
    awaitConnectorIsStarted(connectorName);
    List<Message> testMessages = getTestMessages(connectorName);
    int expectedOffset = testMessages.size() - 1;

    testMessages.forEach(m -> connect.kafka().produce(connectorName, m.key, m.value));
    awaitCommittedOffset(connectorName, connectorName, 0, expectedOffset);

    assertTable().dynamicTableWithPath(getOffsetsTablePath(connectorName)).rows()
        .hasSize(1)
        .allMatch(row -> row.getInt(EColumn.OFFSET.name) == expectedOffset);
    assertTable().dynamicTableWithPath(getOutputTablePath(connectorName)).rows()
        .hasSize(2)
        .are(new Condition<>(RowPredicates.hasDifferentIntValues(EColumn.OFFSET.name),
            "Different offsets in rows"));
  }

  @Test
  void sortedFlattened() {
    String connectorName = "ytsaurus-sorted-flattened";
    connect.kafka().createTopic(connectorName);
    connect.configureConnector(connectorName, getConnectorProps(connectorName));
    awaitConnectorIsStarted(connectorName);
    List<Message> testMessages = getTestMessages(connectorName);
    int expectedOffset = testMessages.size() - 1;

    testMessages.forEach(m -> connect.kafka().produce(connectorName, m.key, m.value));
    awaitCommittedOffset(connectorName, connectorName, 0, expectedOffset);

    assertTable().dynamicTableWithPath(getOffsetsTablePath(connectorName)).rows()
        .hasSize(1)
        .allMatch(row -> row.getInt(EColumn.OFFSET.name) == expectedOffset);
    ListAssert<YTreeMapNode> assertions = assertTable()
        .dynamicTableWithPath(getOutputTablePath(connectorName))
        .rows()
        .hasSize(testMessages.size())
        .are(new Condition<>(RowPredicates.hasDifferentIntValues(EColumn.OFFSET.name),
            "Different offsets in rows"));
    for (int i = 0; i < testMessages.size(); i++) {
      assertions.anyMatch(RowPredicates.hasColumnContaining("id", String.valueOf(i)));
      assertions.anyMatch(RowPredicates.hasColumnContaining("value", String.format("value_%d", i)));
    }
  }

  @Test
  void sortedUnstructuredOperationsSupport() {
    String connectorName = "ytsaurus-sorted-unstructured-ops";
    connect.kafka().createTopic(connectorName);
    connect.configureConnector(connectorName, getConnectorProps(connectorName));
    awaitConnectorIsStarted(connectorName);
    List<Message> testMessages = getTestMessages(connectorName);
    int expectedOffset = testMessages.size() - 1;

    testMessages.forEach(m -> connect.kafka().produce(connectorName, m.key, m.value));
    awaitCommittedOffset(connectorName, connectorName, 0, expectedOffset);

    assertTable().dynamicTableWithPath(getOffsetsTablePath(connectorName)).rows()
        .hasSize(1)
        .allMatch(row -> row.getInt(EColumn.OFFSET.name) == expectedOffset);
    assertTable().dynamicTableWithPath(getOutputTablePath(connectorName)).rows()
        .hasSize(2)
        .anyMatch(RowPredicates.hasColumnContaining(EColumn.KEY.name, "\"id\":\"1\""))
        .anyMatch(RowPredicates.hasColumnContaining(EColumn.KEY.name, "\"id\":\"2\""))
        .anyMatch(RowPredicates.hasColumnContaining(EColumn.DATA.name, "\"value\":\"value_2\""))
        .anyMatch(RowPredicates.hasColumnContaining(EColumn.DATA.name, "\"value\":\"value_4\""))
        .are(new Condition<>(RowPredicates.hasDifferentIntValues(EColumn.OFFSET.name),
            "Different offsets in rows"));
  }

  @Test
  void sortedFlattenedOperationsSupport() {
    String connectorName = "ytsaurus-sorted-flattened-ops";
    connect.kafka().createTopic(connectorName);
    connect.configureConnector(connectorName, getConnectorProps(connectorName));
    awaitConnectorIsStarted(connectorName);
    List<Message> testMessages = getTestMessages(connectorName);
    int expectedOffset = testMessages.size() - 1;

    testMessages.forEach(m -> connect.kafka().produce(connectorName, m.key, m.value));
    awaitCommittedOffset(connectorName, connectorName, 0, expectedOffset);

    assertTable().dynamicTableWithPath(getOffsetsTablePath(connectorName)).rows()
        .hasSize(1)
        .allMatch(row -> row.getInt(EColumn.OFFSET.name) == expectedOffset);
    assertTable().dynamicTableWithPath(getOutputTablePath(connectorName)).rows()
        .hasSize(2)
        .anyMatch(RowPredicates.hasColumnContaining("id", "1"))
        .anyMatch(RowPredicates.hasColumnContaining("id", "2"))
        .anyMatch(RowPredicates.hasColumnContaining("value", "value_2"))
        .anyMatch(RowPredicates.hasColumnContaining("value", "value_4"))
        .are(new Condition<>(RowPredicates.hasDifferentIntValues(EColumn.OFFSET.name),
            "Different offsets in rows"));
  }

  @Test
  void tableRouting() {
    String connectorName = "ytsaurus-table-routing";
    connect.kafka().createTopic(connectorName);
    connect.configureConnector(connectorName, getConnectorProps(connectorName));
    awaitConnectorIsStarted(connectorName);
    List<Message> testMessages = getTestMessages(connectorName);
    int expectedOffset = testMessages.size() - 1;

    testMessages.forEach(m -> connect.kafka().produce(connectorName, m.key, m.value));
    awaitCommittedOffset(connectorName, connectorName, 0, expectedOffset);

    assertTable().dynamicTableWithPath(getOffsetsTablePath(connectorName)).rows()
        .hasSize(1)
        .allMatch(row -> row.getInt(EColumn.OFFSET.name) == expectedOffset);
    assertTable()
        .dynamicTableWithPath(getOutputDir(connectorName) + "/table_1")
        .rows()
        .hasSize(testMessages.size() / 2)
        .are(new Condition<>(RowPredicates.hasDifferentIntValues(EColumn.OFFSET.name),
            "Different offsets in rows"));
    assertTable()
        .dynamicTableWithPath(getOutputDir(connectorName) + "/table_2")
        .rows()
        .hasSize(testMessages.size() / 2)
        .are(new Condition<>(RowPredicates.hasDifferentIntValues(EColumn.OFFSET.name),
            "Different offsets in rows"));
  }

  private Map<String, String> getConnectorProps(String connectorName) {
    Map<String, String> props = baseSinkConnectorProps();
    props.put("value.converter.schemas.enable", "false");
    props.put(SinkConnectorConfig.TOPICS_CONFIG, connectorName);
    props.put(BaseTableWriterConfig.OUTPUT_TYPE, OutputType.DYNAMIC_TABLE.name());
    props.put(BaseTableWriterConfig.OUTPUT_DIRECTORY, getOutputDir(connectorName));
    props.put(StaticTableWriterConfig.ROTATION_PERIOD, "5m");

    switch (connectorName) {
      case "ytsaurus-ordered-unstructured" -> {
        props.put(DynTableWriterConfig.TABLE_TYPE, TableType.ORDERED.name());
        props.put(BaseTableWriterConfig.ROW_MAPPER_CLASS,
            UnstructuredSchemaTableRowMapper.class.getName());
        props.put(BaseTableWriterConfig.KEY_OUTPUT_FORMAT, OutputFormat.STRING.name());
        props.put(BaseTableWriterConfig.VALUE_OUTPUT_FORMAT, OutputFormat.STRING.name());
      }
      case "ytsaurus-ordered-flattened" -> {
        props.put(DynTableWriterConfig.TABLE_TYPE, TableType.ORDERED.name());
        props.put(BaseTableWriterConfig.ROW_MAPPER_CLASS,
            FlattenedSchemaTableRowMapper.class.getName());
      }
      case "ytsaurus-sorted-unstructured", "ytsaurus-sorted-unstructured-ops" -> {
        props.put(DynTableWriterConfig.TABLE_TYPE, TableType.SORTED.name());
        props.put(BaseTableWriterConfig.ROW_MAPPER_CLASS,
            UnstructuredSchemaTableRowMapper.class.getName());
        props.put(BaseTableWriterConfig.KEY_OUTPUT_FORMAT, OutputFormat.STRING.name());
        props.put(BaseTableWriterConfig.VALUE_OUTPUT_FORMAT, OutputFormat.STRING.name());
      }
      case "ytsaurus-sorted-flattened", "ytsaurus-sorted-flattened-ops" -> {
        props.put(DynTableWriterConfig.TABLE_TYPE, TableType.SORTED.name());
        props.put(BaseTableWriterConfig.ROW_MAPPER_CLASS,
            FlattenedSchemaTableRowMapper.class.getName());
      }
      case "ytsaurus-table-routing" -> {
        props.put(DynTableWriterConfig.TABLE_ROUTER_ENABLED, "true");
        props.put(DynTableWriterConfig.TABLE_ROUTER_FIELD, "_col");
      }
    }

    return props;
  }

  private List<Message> getTestMessages(String connectorName) {
    return switch (connectorName) {
      case "ytsaurus-ordered-unstructured", "ytsaurus-sorted-flattened" -> IntStream.range(0, 10)
          .mapToObj(i -> new Message(
              String.format("{\"id\":\"%d\"}", i),
              String.format("{\"value\":\"value_%d\"}", i)))
          .collect(Collectors.toList());
      case "ytsaurus-ordered-flattened" -> IntStream.range(0, 3)
          .mapToObj(i -> new Message(
              String.format("{\"id_%d_col\":\"%d\"}", i, i),
              String.format("{\"value_%d_col\":\"value_%d\"}", i, i)))
          .collect(Collectors.toList());
      case "ytsaurus-sorted-unstructured" -> IntStream.range(0, 10)
          .mapToObj(i -> new Message(
              String.format("{\"id\":\"%d\"}", i % 2),
              String.format("{\"value\":\"value_%d\"}", i)))
          .collect(Collectors.toList());
      case "ytsaurus-sorted-unstructured-ops", "ytsaurus-sorted-flattened-ops" -> List.of(
          new Message("{\"id\":\"1\"}", "{\"value\":\"value_1\",\"_op\":\"c\"}"),
          new Message("{\"id\":\"2\"}", "{\"value\":\"value_2\",\"_op\":\"c\"}"),
          new Message("{\"id\":\"3\"}", "{\"value\":\"value_3\",\"_op\":\"c\"}"),
          new Message("{\"id\":\"1\"}", "{\"value\":\"value_4\",\"_op\":\"u\"}"),
          new Message("{\"id\":\"3\"}", "{\"_op\":\"d\"}"));
      case "ytsaurus-table-routing" -> IntStream.range(0, 10)
          .mapToObj(i -> new Message(
              String.format("{\"id\":\"%d\"}", i),
              String.format("{\"value\":\"value_%d\",\"_col\":\"%s\"}",
                  i, i % 2 == 0 ? "table_1" : "table_2")))
          .collect(Collectors.toList());
      default -> List.of();
    };
  }

  private static String getOutputDir(String connectorName) {
    return String.format("//home/%s/test", connectorName);
  }

  private static String getOffsetsTablePath(String connectorName) {
    return String.format("%s/__connect_sink_metadata__/offsets", getOutputDir(connectorName));
  }

  private static String getOutputTablePath(String connectorName) {
    return String.format("%s/queue", getOutputDir(connectorName));
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

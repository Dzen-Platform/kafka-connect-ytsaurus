package ru.dzen.kafka.connect.ytsaurus.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputFormat;
import ru.dzen.kafka.connect.ytsaurus.serialization.SerializationUtils;
import ru.dzen.kafka.connect.ytsaurus.serialization.TypedNode;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations.TableRow;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations.TableRow.Builder;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;

/**
 * @author pbk-vitaliy
 */
public class FlattenedSchemaTableRowMapper implements TableRowMapper, Configurable {
  private static final Logger log = LoggerFactory.getLogger(FlattenedSchemaTableRowMapper.class);

  public static final String KEY_COLUMNS_INCLUDE = "yt.sink.output.mapper.flattened.key.columns.include";
  public static final String KEY_COLUMNS_EXCLUDE = "yt.sink.output.mapper.flattened.key.columns.exclude";
  public static final String KEY_SOURCE = "yt.sink.output.mapper.flattened.key.source";
  public static final String VALUE_COLUMNS_INCLUDE = "yt.sink.output.mapper.flattened.value.columns.include";
  public static final String VALUE_COLUMNS_EXCLUDE = "yt.sink.output.mapper.flattened.value.columns.exclude";
  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(KEY_COLUMNS_INCLUDE, Type.STRING, "", Importance.MEDIUM, "Comma separated key columns include filter")
      .define(KEY_COLUMNS_EXCLUDE, Type.STRING, "", Importance.MEDIUM, "Comma separated key columns exclude filter")
      .define(KEY_SOURCE, Type.STRING, "key", Importance.MEDIUM, "Source of the key columns. Possible values ('key', 'value')")
      .define(VALUE_COLUMNS_INCLUDE, Type.STRING, "", Importance.MEDIUM, "Comma separated value columns include filter")
      .define(VALUE_COLUMNS_EXCLUDE, Type.STRING, "", Importance.MEDIUM, "Comma separated value columns exclude filter");

  private List<String> keyColumnsOrder = new ArrayList<>();
  private Predicate<String> keyColumnFilter = name -> true;
  private SourceExtractor keySourceExtractor = SourceExtractor.KEY;
  private Predicate<String> valueColumnFilter = name -> true;

  @Override
  public void configure(Map<String, ?> props) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

    String keyColumnsInclude = config.getString(KEY_COLUMNS_INCLUDE);
    if (!keyColumnsInclude.isBlank()) {
      Set<String> includedColumns = new HashSet<>();
      for (String k : keyColumnsInclude.split(",")) {
        keyColumnsOrder.add(k);
        includedColumns.add(k);
      }
      keyColumnFilter = includedColumns::contains;
    }

    String keyColumnsExclude = config.getString(KEY_COLUMNS_EXCLUDE);
    if (!keyColumnsExclude.isBlank()) {
      Set<String> excludedColumns = new HashSet<>(
          Arrays.asList(keyColumnsExclude.split(",")));
      keyColumnFilter = keyColumnFilter.and(name -> !excludedColumns.contains(name));
    }

    keySourceExtractor = SourceExtractor.valueOf(config.getString(KEY_SOURCE).toUpperCase());

    String valueColumnsInclude = config.getString(VALUE_COLUMNS_INCLUDE);
    if (!valueColumnsInclude.isBlank()) {
      Set<String> includedColumns = new HashSet<>(
          Arrays.asList(valueColumnsInclude.split(",")));
      valueColumnFilter = includedColumns::contains;
    }

    String valueColumnsExclude = config.getString(VALUE_COLUMNS_EXCLUDE);
    if (!valueColumnsExclude.isBlank()) {
      Set<String> excludedColumns = new HashSet<>(
          Arrays.asList(valueColumnsExclude.split(",")));
      valueColumnFilter = valueColumnFilter.and(name -> !excludedColumns.contains(name));
    }
  }

  public TableRow recordToRow(SinkRecord record) {
    Map<String, TypedNode> keyColumns;
    try {
      keyColumns = SerializationUtils.serializeToMultipleColumns(
          keySourceExtractor.getSchema(record),
          keySourceExtractor.getValue(record),
          OutputFormat.ANY);
    } catch (Exception e) {
      log.error("Exception in key serialization:", e);
      throw new DataException(e);
    }
    if (keyColumns.isEmpty()) {
      throw new DataException("Expected at least one serialized key column, but got 0");
    }
    Map<String, TypedNode> valueColumns;
    try {
      valueColumns = SerializationUtils.serializeToMultipleColumns(
          record.valueSchema(), record.value(), OutputFormat.ANY);
    } catch (Exception e) {
      log.error("Exception in value serialization:", e);
      throw new DataException(e);
    }
    //TODO rename columns with same names
    TypedNode typedHeadersNode;
    try {
      typedHeadersNode = SerializationUtils.serializeRecordHeaders(record.headers());
    } catch (Exception e) {
      log.error("Exception in headers serialization:", e);
      throw new DataException(e);
    }

    Builder rowBuilder = TableRow.builder();
    Set<String> handledKeyColumns = new HashSet<>();
    for (String key : keyColumnsOrder) {
      if (keyColumnFilter.test(key)) {
        TypedNode node = keyColumns.get(key);
        if (node != null) {
          rowBuilder.addKeyColumn(key, node.getType(), node.getValue());
        }
        handledKeyColumns.add(key);
      }
    }
    keyColumns.keySet().stream()
        .filter(keyColumnFilter)
        .filter(k -> !handledKeyColumns.contains(k))
        .sorted()
        .forEach(name -> rowBuilder.addKeyColumn(
            name,
            keyColumns.get(name).getType(),
            keyColumns.get(name).getValue()
        ));
    valueColumns.keySet().stream()
        .filter(valueColumnFilter)
        .sorted()
        .forEach(name -> rowBuilder.addValueColumn(
            name,
            valueColumns.get(name).getType(),
            valueColumns.get(name).getValue()
        ));
    rowBuilder.addValueColumn(
        UnstructuredTableSchema.EColumn.TOPIC.name,
        TiType.string(),
        YTree.stringNode(record.topic())
    );
    rowBuilder.addValueColumn(
        UnstructuredTableSchema.EColumn.PARTITION.name,
        TiType.uint64(),
        YTree.unsignedLongNode(record.kafkaPartition())
    );
    rowBuilder.addValueColumn(
        UnstructuredTableSchema.EColumn.OFFSET.name,
        TiType.uint64(),
        YTree.unsignedLongNode(record.kafkaOffset())
    );
    rowBuilder.addValueColumn(
        UnstructuredTableSchema.EColumn.TIMESTAMP.name,
        TiType.uint64(),
        YTree.unsignedLongNode(System.currentTimeMillis())
    );
    rowBuilder.addValueColumn(
        UnstructuredTableSchema.EColumn.HEADERS.name,
        typedHeadersNode.getType(),
        typedHeadersNode.getValue()
    );
    return rowBuilder.build();
  }

  private enum SourceExtractor {
    KEY(ConnectRecord::keySchema, ConnectRecord::key),
    VALUE(ConnectRecord::valueSchema, ConnectRecord::value),
    ;
    private final Function<SinkRecord, Schema> schemaExtractor;
    private final Function<SinkRecord, Object> valueExtractor;

    SourceExtractor(
        Function<SinkRecord, Schema> schemaExtractor,
        Function<SinkRecord, Object> valueExtractor) {
      this.schemaExtractor = schemaExtractor;
      this.valueExtractor = valueExtractor;
    }

    public Schema getSchema(SinkRecord record) {
      return schemaExtractor.apply(record);
    }

    public Object getValue(SinkRecord record) {
      return valueExtractor.apply(record);
    }
  }
}

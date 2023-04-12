package ru.dzen.kafka.connect.ytsaurus.common;


import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.TiType;

public final class UnstructuredTableSchema {

  public static final TableSchema OFFSETS_TABLE_SCHEMA = TableSchema.builder()
      .addKey(EColumn.TOPIC.name, TiType.string())
      .addKey(EColumn.PARTITION.name, TiType.uint32())
      .addValue(EColumn.OFFSET.name, TiType.uint64())
      .build();

  public static TableSchema createDataQueueTableSchema(
      BaseTableWriterConfig.OutputFormat keyOutputFormat,
      BaseTableWriterConfig.OutputFormat valueOutputFormat, Map<EColumn, String> metadataColumns) {
    var builder = TableSchema.builder()
        .setUniqueKeys(false);
    if (keyOutputFormat != null) {
      builder.addValue(EColumn.KEY.name, keyOutputFormat.toTiType());
    }
    if (valueOutputFormat != null) {
      builder.addValue(EColumn.DATA.name, valueOutputFormat.toTiType());
    }
    if (metadataColumns.containsKey(EColumn.TOPIC)) {
      builder.addValue(metadataColumns.get(EColumn.TOPIC), TiType.string());
    }
    if (metadataColumns.containsKey(EColumn.PARTITION)) {
      builder.addValue(metadataColumns.get(EColumn.PARTITION), TiType.uint64());
    }
    if (metadataColumns.containsKey(EColumn.OFFSET)) {
      builder.addValue(metadataColumns.get(EColumn.OFFSET), TiType.uint64());
    }
    if (metadataColumns.containsKey(EColumn.TIMESTAMP)) {
      builder.addValue(metadataColumns.get(EColumn.TIMESTAMP), TiType.uint64());
    }
    if (metadataColumns.containsKey(EColumn.HEADERS)) {
      builder.addValue(metadataColumns.get(EColumn.HEADERS), TiType.optional(TiType.yson()));
    }

    return builder.build();
  }

  public enum ETableType {
    STATIC,
    DYNAMIC
  }

  public enum EColumn {
    DATA("data", TiType.string(), false),
    KEY("_key", TiType.string(), true),
    TOPIC("_topic", TiType.string(), true),
    PARTITION("_partition", TiType.uint64(), true),
    OFFSET("_offset", TiType.uint64(), true),
    TIMESTAMP("_timestamp", TiType.uint64(), true),
    HEADERS("_headers", TiType.optional(TiType.yson()), true),
    SYSTEM_TIMESTAMP("_timestamp", TiType.uint64(), true, ETableType.DYNAMIC),
    SYSTEM_CUMULATIVE_DATA_WEIGHT("_timestamp", TiType.uint64(), true, ETableType.DYNAMIC);

    public final String name;

    public final TiType type;
    public final boolean isMetadata;
    private final Set<ETableType> tableTypes;

    // Constructor
    EColumn(String value, TiType type, boolean isMetadata, ETableType... tableTypes) {
      this.name = value;
      this.type = type;
      this.isMetadata = isMetadata;
      this.tableTypes = Arrays.stream(tableTypes).collect(Collectors.toSet());
    }

    EColumn(String value, TiType type, boolean isMetadata) {
      this(value, type, isMetadata, ETableType.STATIC, ETableType.DYNAMIC);
    }

    public static Map<EColumn, String> getAllMetadataColumns(ETableType tableType) {
      return Arrays.stream(values())
          .filter(column -> column.isMetadata && column.tableTypes.contains(tableType))
          .collect(Collectors.toMap(column -> column, column -> column.name, (c1, c2) -> c1,
              TreeMap::new));
    }
  }
}
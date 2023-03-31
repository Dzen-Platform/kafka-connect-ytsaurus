package ru.dzen.kafka.connect.ytsaurus.common;


import java.util.Set;
import java.util.TreeSet;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.TiType;

public final class UnstructuredTableSchema {

  public static final TableSchema offsetsTableSchema = TableSchema.builder()
      .addKey(EColumn.TOPIC.name, TiType.string())
      .addKey(EColumn.PARTITION.name, TiType.uint32())
      .addValue(EColumn.OFFSET.name, TiType.uint64())
      .build();

  public static TableSchema createDataQueueTableSchemaWithSelectedMetadata(TiType keyType,
      TiType dataType, Set<EColumn> metadataColumns) {
    var builder = TableSchema.builder()
        .setUniqueKeys(false);
    builder.addValue(EColumn.KEY.name, keyType)
        .addValue(EColumn.DATA.name, dataType);
    if (metadataColumns.contains(EColumn.TOPIC)) {
      builder.addValue(EColumn.TOPIC.name, TiType.string());
    }
    if (metadataColumns.contains(EColumn.PARTITION)) {
      builder.addValue(EColumn.PARTITION.name, TiType.uint64());
    }
    if (metadataColumns.contains(EColumn.OFFSET)) {
      builder.addValue(EColumn.OFFSET.name, TiType.uint64());
    }
    if (metadataColumns.contains(EColumn.TIMESTAMP)) {
      builder.addValue(EColumn.TIMESTAMP.name, TiType.uint64());
    }
    if (metadataColumns.contains(EColumn.HEADERS)) {
      builder.addValue(EColumn.HEADERS.name, TiType.optional(TiType.yson()));
    }

    return builder.build();
  }

  public static TableSchema createDataQueueTableSchema(TiType keyType, TiType dataType) {
    return createDataQueueTableSchemaWithSelectedMetadata(keyType, dataType,
        EColumn.getAllMetadataColumns());
  }

  public static TableSchema createDataQueueTableSchema(
      BaseTableWriterConfig.OutputFormat keyOutputFormat,
      BaseTableWriterConfig.OutputFormat valueOutputFormat) {
    TiType keyType = keyOutputFormat.toTiType();
    TiType dataType = valueOutputFormat.toTiType();

    return createDataQueueTableSchema(keyType, dataType);
  }

  public enum EColumn {
    DATA("data", TiType.string(), false),
    KEY("_key", TiType.string(), true),
    TOPIC("_topic", TiType.string(), true),
    PARTITION("_partition", TiType.uint64(), true),
    OFFSET("_offset", TiType.uint64(), true),
    TIMESTAMP("_timestamp", TiType.uint64(), true),
    HEADERS("_headers", TiType.optional(TiType.yson()), true);

    public final String name;

    public final TiType type;
    public final boolean isMetadata;

    // Constructor
    EColumn(String value, TiType type, boolean isMetadata) {
      this.name = value;
      this.type = type;
      this.isMetadata = isMetadata;
    }

    // Getter
    static Set<EColumn> getAllMetadataColumns() {
      var columns = new TreeSet<EColumn>();
      for (EColumn column : values()) {
        if (column.isMetadata) {
          columns.add(column);
        }
      }
      return columns;
    }
  }
}
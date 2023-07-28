package ru.dzen.kafka.connect.ytsaurus.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnSortOrder;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * @author pbk-vitaliy
 */
public class TableRow {

  private final TableSchema rowSchema;
  private final Map<String, YTreeNode> key;
  private final Map<String, YTreeNode> value;

  public TableRow(TableSchema rowSchema, Map<String, YTreeNode> key, Map<String, YTreeNode> value) {
    this.rowSchema = rowSchema;
    this.key = key;
    this.value = value;
  }

  public static Builder builder() {
    return new Builder();
  }

  public TableSchema getSchema() {
    return rowSchema;
  }

  public Map<String, YTreeNode> getKey() {
    return key;
  }

  public Map<String, YTreeNode> getValue() {
    return value;
  }

  public Map<String, YTreeNode> asMap() {
    HashMap<String, YTreeNode> row = new HashMap<>(value);
    row.putAll(key);
    return row;
  }

  public static class Builder {
    private final List<ColumnSchema> columns = new ArrayList<>();
    private final Map<String, YTreeNode> keyMap = new HashMap<>();
    private final Map<String, YTreeNode> valueMap = new HashMap<>();
    private ColumnSortOrder keyColumnsSortOrder;

    public Builder setKeyColumnsSortOrder(@Nullable ColumnSortOrder columnSortOrder) {
      keyColumnsSortOrder = columnSortOrder;
      return this;
    }

    public Builder addKeyColumn(String columnName, TiType type, YTreeNode value) {
      columns.add(new ColumnSchema(columnName, type, keyColumnsSortOrder));
      keyMap.put(columnName, value);
      return this;
    }

    public Builder addValueColumn(String columnName, TiType type, YTreeNode value) {
      columns.add(new ColumnSchema(columnName, type));
      valueMap.put(columnName, value);
      return this;
    }

    public TableRow build() {
      TableSchema rowSchema = TableSchema.builder()
          .addAll(columns)
          .build();
      return new TableRow(rowSchema, keyMap, valueMap);
    }
  }
}

package ru.dzen.kafka.connect.ytsaurus.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.errors.SchemaBuilderException;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.DynTableWriterConfig.TableType;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnSortOrder;
import tech.ytsaurus.core.tables.TableSchema;

/**
 * @author pbk-vitaliy
 */
public class SchemaUtils {

  public static TableSchema mergeSchemas(TableSchema currentSchema, TableSchema updatedSchema) {
    List<ColumnSchema> resultSchema = new ArrayList<>();
    List<ColumnSchema> currentKeys = getKeyColumns(currentSchema);
    List<ColumnSchema> updatedKeys = getKeyColumns(updatedSchema);
    mergeColumns(resultSchema, currentKeys, updatedKeys);
    List<ColumnSchema> currentValueColumns = getValueColumns(currentSchema);
    List<ColumnSchema> updatedValueColumns = getValueColumns(updatedSchema);
    mergeColumns(resultSchema, currentValueColumns, updatedValueColumns);
    return TableSchema.builder()
        .addAll(resultSchema)
        .build();
  }

  public static TableSchema transformSchema(TableSchema schema, TableType tableType) {
    if (schema.getColumns().isEmpty()) {
      return schema;
    }
    if (tableType == TableType.ORDERED && hasSortedColumn(schema)) {
      List<ColumnSchema> columns = schema.getColumns().stream()
          .map(col -> col.toBuilder().setSortOrder(null).build())
          .collect(Collectors.toList());
      return schema.toBuilder()
          .setColumns(columns)
          .setUniqueKeys(false)
          .build();
    }
    if (tableType == TableType.SORTED && !hasSortedColumn(schema)) {
      List<ColumnSchema> columns = new ArrayList<>(schema.getColumns());
      columns.set(0, columns.get(0).toBuilder().setSortOrder(ColumnSortOrder.ASCENDING).build());
      return schema.toBuilder()
          .setColumns(columns)
          .setUniqueKeys(true)
          .build();
    }
    return schema;
  }

  private static List<ColumnSchema> getKeyColumns(TableSchema schema) {
    List<ColumnSchema> result = new ArrayList<>();
    for (ColumnSchema column : schema.getColumns()) {
      ColumnSortOrder sortOrder = column.getSortOrder();
      if (sortOrder == null) {
        break;
      }
      result.add(column);
    }
    return result;
  }

  private static List<ColumnSchema> getValueColumns(TableSchema schema) {
    List<ColumnSchema> result = new ArrayList<>();
    for (ColumnSchema column : schema.getColumns()) {
      ColumnSortOrder sortOrder = column.getSortOrder();
      if (sortOrder == null) {
        result.add(column);
      }
    }
    return result;
  }

  private static void mergeColumns(
      List<ColumnSchema> resultSchema,
      List<ColumnSchema> currentColumns,
      List<ColumnSchema> updatedColumns) {
    Map<String, ColumnSchema> currentColumnsByName = resultSchema.stream()
        .collect(Collectors.toMap(ColumnSchema::getName, Function.identity()));
    for (ColumnSchema column : currentColumns) {
      ColumnSchema currCol = currentColumnsByName.get(column.getName());
      if (currCol == null) {
        resultSchema.add(column);
        currentColumnsByName.put(column.getName(), column);
        continue;
      }
      if (!currCol.getTypeV3().equals(column.getTypeV3())) {
        throw new SchemaBuilderException("Column type change not supported");
      }
    }
    for (ColumnSchema column : updatedColumns) {
      ColumnSchema currCol = currentColumnsByName.get(column.getName());
      if (currCol == null) {
        resultSchema.add(column);
        currentColumnsByName.put(column.getName(), column);
        continue;
      }
      if (!currCol.getTypeV3().equals(column.getTypeV3())) {
        throw new SchemaBuilderException("Column type change not supported");
      }
    }
  }

  private static boolean hasSortedColumn(TableSchema schema) {
    return schema.getColumns().stream()
        .anyMatch(c -> c.getSortOrder() != null);
  }
}

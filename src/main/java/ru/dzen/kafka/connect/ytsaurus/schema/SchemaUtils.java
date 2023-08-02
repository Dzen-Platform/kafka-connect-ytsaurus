package ru.dzen.kafka.connect.ytsaurus.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.errors.SchemaBuilderException;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnSortOrder;
import tech.ytsaurus.core.tables.TableSchema;

/**
 * @author pbk-vitaliy
 */
public class SchemaUtils {

  public static TableSchema mergeSchemas(TableSchema currentSchema, TableSchema updatedSchema) {
    if (currentSchema == null) {
      return updatedSchema;
    }
    if (updatedSchema == null || currentSchema.equals(updatedSchema)) {
      return currentSchema;
    }
    List<ColumnSchema> resultSchema = new ArrayList<>();
    List<ColumnSchema> currentKeys = getKeyColumns(currentSchema);
    List<ColumnSchema> updatedKeys = getKeyColumns(updatedSchema);
    mergeColumns(resultSchema, currentKeys, updatedKeys);
    List<ColumnSchema> currentValueColumns = getValueColumns(currentSchema);
    List<ColumnSchema> updatedValueColumns = getValueColumns(updatedSchema);
    mergeColumns(resultSchema, currentValueColumns, updatedValueColumns);
    return TableSchema.builder()
        .setUniqueKeys(currentSchema.isUniqueKeys() || updatedSchema.isUniqueKeys())
        .setStrict(currentSchema.isStrict() || updatedSchema.isStrict())
        .addAll(resultSchema)
        .build();
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

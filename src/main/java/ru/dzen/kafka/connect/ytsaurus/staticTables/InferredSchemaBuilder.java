package ru.dzen.kafka.connect.ytsaurus.staticTables;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTreeIntegerNode;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

public class InferredSchemaBuilder {

  private final Map<String, TiType> inferredSchema;

  public InferredSchemaBuilder() {
    inferredSchema = new HashMap<>();
  }

  public InferredSchemaBuilder(YTreeNode schemaNode) {
    inferredSchema = new HashMap<>();
    for (var columnSchema : TableSchema.fromYTree(schemaNode).getColumns()) {
      inferredSchema.put(columnSchema.getName(), columnSchema.getTypeV3());
    }
  }

  public InferredSchemaBuilder(TableSchema schema) {
    this(schema.toYTree());
  }

  public InferredSchemaBuilder(List<YTreeNode> schemaNodes) {
    inferredSchema = new HashMap<>();
    for (var node : schemaNodes) {
      var schemaBuilder = new InferredSchemaBuilder(node);
      for (var entry : schemaBuilder.inferredSchema.entrySet()) {
        updateSchema(entry.getKey(), entry.getValue());
      }
    }
  }

  public void update(YTreeMapNode mapNode) {
    for (String key : mapNode.asMap().keySet()) {
      var valueType = detectColumnType(mapNode.get(key).get());
      updateSchema(key, valueType);
    }
  }

  public void update(List<YTreeMapNode> mapNodes) {
    for (var mapNode : mapNodes) {
      update(mapNode);
    }
  }

  public TableSchema build() {
    var builder = TableSchema.builder();
    for (Map.Entry<String, TiType> entry : inferredSchema.entrySet()) {
      builder.addValue(entry.getKey(), entry.getValue());
    }
    return builder.build();
  }

  private TiType detectColumnType(YTreeNode value) {
    if (value.isStringNode()) {
      return TiType.optional(TiType.string());
    } else if (value.isIntegerNode()) {
      if (((YTreeIntegerNode) value).isSigned()) {
        return TiType.optional(TiType.int64());
      }
      return TiType.optional(TiType.uint64());
    } else if (value.isDoubleNode()) {
      return TiType.optional(TiType.doubleType());
    } else if (value.isBooleanNode()) {
      return TiType.optional(TiType.bool());
    } else {
      return TiType.optional(TiType.yson());
    }
  }

  private void updateSchema(String key, TiType valueType) {
    var existingType = inferredSchema.get(key);
    if (existingType == null) {
      inferredSchema.put(key, valueType);
    } else if (!existingType.equals(valueType)) {
      inferredSchema.put(key, TiType.optional(TiType.yson()));
    }
  }
}

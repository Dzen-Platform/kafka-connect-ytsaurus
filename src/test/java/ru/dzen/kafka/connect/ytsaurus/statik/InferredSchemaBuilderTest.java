package ru.dzen.kafka.connect.ytsaurus.staticTables;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;


public class InferredSchemaBuilderTest {

  @org.junit.jupiter.api.Test
  void testEmptyConstructor() {
    InferredSchemaBuilder builder = new InferredSchemaBuilder();
    TableSchema schema = builder.build();
    assertEquals(0, schema.getColumns().size());
  }

  @org.junit.jupiter.api.Test
  void testYTreeNodeConstructor() {
    // Create schema using TableSchema.builder()
    TableSchema tableSchema = TableSchema.builder()
        .addValue("column1", TiType.optional(TiType.string()))
        .addValue("column2", TiType.optional(TiType.int64()))
        .build();

    // Convert schema to YTreeNode
    YTreeNode schemaNode = tableSchema.toYTree();

    InferredSchemaBuilder builder = new InferredSchemaBuilder(schemaNode);
    TableSchema schema = builder.build();
    assertEquals(2, schema.getColumns().size());
    assertEquals(TiType.optional(TiType.string()),
        schema.getColumnSchema(schema.findColumn("column1")).getTypeV3());
    assertEquals(TiType.optional(TiType.int64()),
        schema.getColumnSchema(schema.findColumn("column2")).getTypeV3());
  }

  @org.junit.jupiter.api.Test
  void testTableSchemaConstructor() {
    TableSchema inputSchema = TableSchema.builder()
        .addValue("column1", TiType.optional(TiType.string()))
        .addValue("column2", TiType.optional(TiType.int64()))
        .build();
    InferredSchemaBuilder builder = new InferredSchemaBuilder(inputSchema);
    TableSchema schema = builder.build();
    assertEquals(2, schema.getColumns().size());
    assertEquals(TiType.optional(TiType.string()),
        schema.getColumnSchema(schema.findColumn("column1")).getTypeV3());
    assertEquals(TiType.optional(TiType.int64()),
        schema.getColumnSchema(schema.findColumn("column2")).getTypeV3());
  }

  @org.junit.jupiter.api.Test
  void testListYTreeNodeConstructor() {
    // Create schema 1 using TableSchema.builder()
    TableSchema schema1 = TableSchema.builder()
        .addValue("column1", TiType.optional(TiType.string()))
        .addValue("column2", TiType.optional(TiType.int64()))
        .build();

    // Convert schema1 to YTreeNode
    YTreeNode schemaNode1 = schema1.toYTree();

    // Create schema 2 using TableSchema.builder()
    TableSchema schema2 = TableSchema.builder()
        .addValue("column2", TiType.optional(TiType.int64()))
        .addValue("column3", TiType.optional(TiType.doubleType()))
        .build();

    // Convert schema2 to YTreeNode
    YTreeNode schemaNode2 = schema2.toYTree();

    var schemaNodes = new ArrayList<YTreeNode>();
    schemaNodes.add(schemaNode1);
    schemaNodes.add(schemaNode2);

    InferredSchemaBuilder builder = new InferredSchemaBuilder(schemaNodes);
    TableSchema schema = builder.build();
    assertEquals(3, schema.getColumns().size());
    assertEquals(TiType.optional(TiType.string()),
        schema.getColumnSchema(schema.findColumn("column1")).getTypeV3());
    assertEquals(TiType.optional(TiType.int64()),
        schema.getColumnSchema(schema.findColumn("column2")).getTypeV3());
    assertEquals(TiType.optional(TiType.doubleType()),
        schema.getColumnSchema(schema.findColumn("column3")).getTypeV3());
  }

  @org.junit.jupiter.api.Test
  void testUpdateYTreeMapNode() {
    InferredSchemaBuilder builder = new InferredSchemaBuilder();
    var mapNodeBuilder = YTree.mapBuilder()
        .key("column1").value(YTree.stringNode("value"))
        .key("column2").value(YTree.longNode(42))
        .key("column3").value(YTree.doubleNode(3.14));
    YTreeMapNode mapNode = mapNodeBuilder.buildMap();

    builder.update(mapNode);
    TableSchema schema = builder.build();
    assertEquals(3, schema.getColumns().size());
    assertEquals(TiType.optional(TiType.string()),
        schema.getColumnSchema(schema.findColumn("column1")).getTypeV3());
    assertEquals(TiType.optional(TiType.int64()),
        schema.getColumnSchema(schema.findColumn("column2")).getTypeV3());
    assertEquals(TiType.optional(TiType.doubleType()),
        schema.getColumnSchema(schema.findColumn("column3")).getTypeV3());
  }

  @org.junit.jupiter.api.Test
  void testUpdateListOfYTreeMapNodes() {
    InferredSchemaBuilder builder = new InferredSchemaBuilder();
    var mapNodeBuilder1 = YTree.mapBuilder()
        .key("column1").value(YTree.stringNode("value"))
        .key("column2").value(YTree.longNode(42));
    YTreeMapNode mapNode1 = mapNodeBuilder1.buildMap();

    var mapNodeBuilder2 = YTree.mapBuilder()
        .key("column2").value(YTree.longNode(42))
        .key("column3").value(YTree.doubleNode(3.14));
    YTreeMapNode mapNode2 = mapNodeBuilder2.buildMap();

    var mapNodes = new ArrayList<YTreeMapNode>();
    mapNodes.add(mapNode1);
    mapNodes.add(mapNode2);

    builder.update(mapNodes);
    TableSchema schema = builder.build();
    assertEquals(3, schema.getColumns().size());
    assertEquals(TiType.optional(TiType.string()),
        schema.getColumnSchema(schema.findColumn("column1")).getTypeV3());
    assertEquals(TiType.optional(TiType.int64()),
        schema.getColumnSchema(schema.findColumn("column2")).getTypeV3());
    assertEquals(TiType.optional(TiType.doubleType()),
        schema.getColumnSchema(schema.findColumn("column3")).getTypeV3());
  }

  @org.junit.jupiter.api.Test
  void testInferredSchemaUpdate() {
    InferredSchemaBuilder builder = new InferredSchemaBuilder();
    var mapNodeBuilder1 = YTree.mapBuilder()
        .key("column1").value(YTree.stringNode("value"))
        .key("column2").value(YTree.longNode(42));
    YTreeMapNode mapNode1 = mapNodeBuilder1.buildMap();

    var mapNodeBuilder2 = YTree.mapBuilder()
        .key("column1").value(YTree.longNode(1))
        .key("column2").value(YTree.stringNode("text"))
        .key("column3").value(YTree.doubleNode(3.14));
    YTreeMapNode mapNode2 = mapNodeBuilder2.buildMap();

    builder.update(mapNode1);
    builder.update(mapNode2);
    TableSchema schema = builder.build();
    assertEquals(3, schema.getColumns().size());
    assertEquals(TiType.optional(TiType.yson()),
        schema.getColumnSchema(schema.findColumn("column1")).getTypeV3());
    assertEquals(TiType.optional(TiType.yson()),
        schema.getColumnSchema(schema.findColumn("column2")).getTypeV3());
    assertEquals(TiType.optional(TiType.doubleType()),
        schema.getColumnSchema(schema.findColumn("column3")).getTypeV3());
  }
}
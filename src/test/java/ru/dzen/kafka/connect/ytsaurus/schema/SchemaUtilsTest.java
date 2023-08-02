package ru.dzen.kafka.connect.ytsaurus.schema;

import static org.assertj.core.api.Assertions.assertThatException;

import org.assertj.core.api.AbstractObjectAssert;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.TiType;

/**
 * @author pbk-vitaliy
 */
public class SchemaUtilsTest {

  @Test
  void mergeSchemaWithNull() {
    TableSchema schema = TableSchema.builder()
        .addValue("column", TiType.string())
        .build();

    TableSchema merged = SchemaUtils.mergeSchemas(schema, null);

    assertThat(merged).isEqualTo(schema);
  }

  @Test
  void mergeNullWithSchema() {
    TableSchema schema = TableSchema.builder()
        .addValue("column", TiType.string())
        .build();

    TableSchema merged = SchemaUtils.mergeSchemas(null, schema);

    assertThat(merged).isEqualTo(schema);
  }

  @Test
  void mergeEmptySchemas() {
    TableSchema empty1 = TableSchema.builder().build();
    TableSchema empty2 = TableSchema.builder().build();

    TableSchema merged = SchemaUtils.mergeSchemas(empty1, empty2);

    assertThat(merged).isEqualTo(empty1);
  }

  @Test
  void mergeSchemasWithoutSorting() {
    TableSchema schema1 = TableSchema.builder()
        .addValue("column1", TiType.string())
        .build();
    TableSchema schema2 = TableSchema.builder()
        .addValue("column1", TiType.string())
        .addValue("column2", TiType.doubleType())
        .build();

    TableSchema merged = SchemaUtils.mergeSchemas(schema1, schema2);

    assertThat(merged).hasColumnWithType("column1", TiType.string());
    assertThat(merged).hasColumnWithType("column2", TiType.doubleType());
  }

  @Test
  void columnsOrderNotChanges() {
    TableSchema schema1 = TableSchema.builder()
        .addKey("column2", TiType.string())
        .addKey("column3", TiType.string())
        .addValue("column5", TiType.string())
        .build();
    TableSchema schema2 = TableSchema.builder()
        .addValue("column1", TiType.string())
        .addValue("column4", TiType.doubleType())
        .build();

    TableSchema merged = SchemaUtils.mergeSchemas(schema1, schema2);

    assertThat(merged).hasColumnsInOrder("column2", "column3", "column5", "column1", "column4");
  }

  @Test
  void newKeyColumnsAddsBeforeValueColumns() {
    TableSchema schema1 = TableSchema.builder()
        .addKey("column1", TiType.string())
        .addValue("column2", TiType.string())
        .build();
    TableSchema schema2 = TableSchema.builder()
        .addKey("column3", TiType.string())
        .build();

    TableSchema merged = SchemaUtils.mergeSchemas(schema1, schema2);

    assertThat(merged).hasColumnsInOrder("column1", "column3", "column2");
  }

  @Test
  void throwsExceptionOnColumnTypeChange() {
    TableSchema schema1 = TableSchema.builder()
        .addValue("column1", TiType.string())
        .build();
    TableSchema schema2 = TableSchema.builder()
        .addValue("column1", TiType.doubleType())
        .build();

    assertThatException().isThrownBy(() -> SchemaUtils.mergeSchemas(schema1, schema2));
  }

  private static SchemaAssert assertThat(TableSchema schema) {
    return new SchemaAssert(schema);
  }

  private static final class SchemaAssert extends AbstractObjectAssert<SchemaAssert, TableSchema> {

    public SchemaAssert(TableSchema tableSchema) {
      super(tableSchema, SchemaAssert.class);
    }

    public SchemaAssert hasColumnWithType(String column, TiType type) {
      int columnIdx;
      if ((columnIdx = actual.findColumn(column)) == -1) {
        throw failure("Expected schema <%s> to contain column with name <%s> but was not",
            actual, column);
      }
      TiType actualType;
      if (!(actualType = actual.getColumnSchema(columnIdx).getTypeV3()).equals(type)) {
        throw failure("Expected column <%s> type will be <%s> type but actually has <%s>",
            column, type, actualType);
      }
      return myself;
    }

    public SchemaAssert hasColumnsInOrder(String... columns) {
      if (actual.getColumns().size() < columns.length) {
        throw failure("Expected schema <%s> has columns in order <%s> but was not",
            actual, columns);
      }
      for (int i = 0; i < columns.length; i++) {
        if (!actual.getColumnName(i).equals(columns[i])) {
          throw failure("Expected schema <%s> has columns in order <%s> but was not",
              actual, columns);
        }
      }
      return myself;
    }
  }
}

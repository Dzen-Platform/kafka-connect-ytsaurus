package ru.dzen.kafka.connect.ytsaurus.staticTables;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriter;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputTableSchemaType;
import ru.dzen.kafka.connect.ytsaurus.common.TableWriterManager;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema.EColumn;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema.ETableType;
import ru.dzen.kafka.connect.ytsaurus.common.Util;
import ru.dzen.kafka.connect.ytsaurus.staticTables.StaticTableWriterConfig.SchemaInferenceStrategy;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.Format;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.request.LockMode;
import tech.ytsaurus.core.rows.YTreeMapNodeSerializer;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

public class StaticTableWriter extends BaseTableWriter {

  private static final Logger log = LoggerFactory.getLogger(StaticTableWriter.class);

  public final StaticTableWriterConfig config;
  protected final SchemaManager schemaManager;
  private final TableSchema unstructuredTableSchema;

  public StaticTableWriter(StaticTableWriterConfig config) {
    super(config, new DocumentOffsetsManager(config.getOffsetsDirectory()));
    this.config = config;

    unstructuredTableSchema = UnstructuredTableSchema.createDataQueueTableSchema(
        config.getKeyOutputFormat(), config.getValueOutputFormat(), EColumn.getAllMetadataColumns(
            ETableType.STATIC));

    schemaManager = new SchemaManager(config.getSchemasDirectory());
  }

  protected Instant getNow() throws Exception {
    return client.generateTimestamps().get().getInstant();
  }

  protected YPath getOutputTablePath(Instant now) {
    var rotationPeriod = config.getRotationPeriod();
    var dateOnly = rotationPeriod.getSeconds() % (60 * 60 * 25) == 0;
    var tableName = Util.formatDateTime(Util.floorByDuration(now, rotationPeriod), dateOnly);
    return YPath.simple(config.getOutputTablesDirectory() + "/" + tableName);
  }

  @Override
  protected void writeRows(ApiServiceTransaction trx, Collection<SinkRecord> records,
      Set<TopicPartition> topicPartitions)
      throws Exception {
    var tableRows = recordsToRows(records);
    var rows = tableRows.stream().map(x -> (YTreeMapNode) YTree.node(x.asMap()))
        .collect(Collectors.toList());

    var inferredSchemaBuilder = new InferredSchemaBuilder();
    if (config.getSchemaInferenceStrategy()
        .equals(SchemaInferenceStrategy.INFER_FROM_FINALIZED_TABLE)) {
      inferredSchemaBuilder = new InferredSchemaBuilder(schemaManager.getPrevSchema(trx));
      inferredSchemaBuilder.update(rows);
    }

    var outputPath = getOutputTablePath(getNow());
    if (trx.existsNode(outputPath.toString()).get()) {
      trx.lockNode(outputPath.toString(), LockMode.Shared).get();
      if (trx.getNode(outputPath.allAttributes().toString()).get().mapNode().get("final")
          .isPresent()) {
        throw new RuntimeException("Tried to modify finalized table!");
      }
    } else {
      var createNodeBuilder = CreateNode.builder().setType(CypressNodeType.TABLE)
          .setPath(outputPath);
      if (config.getSchemaInferenceStrategy()
          .equals(SchemaInferenceStrategy.INFER_FROM_FIRST_BATCH)) {
        inferredSchemaBuilder.update(rows);
        var inferredSchema = inferredSchemaBuilder.build();
        log.warn(
            "Automatically inferred schema with %d columns from first {} rows: {}",
            inferredSchema.getColumns().size(), rows.size(),
            inferredSchema.toYTree().toString());
        createNodeBuilder.setAttributes(Map.of("schema", inferredSchema.toYTree()));
      } else if (config.getOutputTableSchemaType().equals(OutputTableSchemaType.UNSTRUCTURED)) {
        createNodeBuilder.setAttributes(Map.of("schema", unstructuredTableSchema.toYTree()));
      } else if (config.getOutputTableSchemaType().equals(OutputTableSchemaType.STRICT)
          && !config.getSchemaInferenceStrategy()
          .equals(SchemaInferenceStrategy.INFER_FROM_FINALIZED_TABLE)) {
        createNodeBuilder.setAttributes(
            Map.of("schema", createStrictTableSchemaFromRecordsSchema(records).toYTree()));
      }
      for (var entry : config.getExtraTablesAttributes().entrySet()) {
        createNodeBuilder.addAttribute(entry.getKey(), entry.getValue());
      }
      trx.createNode(createNodeBuilder.build()).get();
    }

    var writeTable = WriteTable.<YTreeMapNode>builder()
        .setPath(outputPath.plusAdditionalAttribute("append", true))
        .setSerializationContext(
            new SerializationContext<>(new YTreeMapNodeSerializer(), Format.ysonBinary()))
        .build();
    var writer = trx.writeTable(writeTable).get();

    try {
      while (true) {
        // It is necessary to wait for readyEvent before trying to write.
        writer.readyEvent().get();

        // If false is returned, then readyEvent must be waited for before trying again.
        var accepted = config.getOutputTableSchemaType().equals(OutputTableSchemaType.WEAK)
            ? writer.write(rows)
            : writer.write(rows, unstructuredTableSchema);

        if (accepted) {
          break;
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      // Waiting for completion of writing. An exception might be thrown if something goes wrong.
      writer.close().get();
    }

    if (config.getSchemaInferenceStrategy()
        .equals(SchemaInferenceStrategy.INFER_FROM_FINALIZED_TABLE)) {
      schemaManager.writeSchema(trx, inferredSchemaBuilder.build(), topicPartitions);
    }
  }

  private TableSchema createStrictTableSchemaFromRecordsSchema(Collection<SinkRecord> records) {
    var tableSchemaBuilder = UnstructuredTableSchema.createDataQueueTableSchema(
            config.getKeyOutputFormat(), null, EColumn.getAllMetadataColumns(ETableType.STATIC))
        .toBuilder();
    var firstRecord = records.stream().iterator().next();
    var firstRecordValueSchema = firstRecord.valueSchema();
    for (var field : firstRecordValueSchema.fields()) {
      var columnType = recordSchemaTypeToColumnType(field.schema().type());
      if (field.schema().isOptional() || columnType.isYson()) {
        columnType = TiType.optional(columnType);
      }
      tableSchemaBuilder.addValue(field.name(), columnType);
    }
    return tableSchemaBuilder.build();
  }

  private TiType recordSchemaTypeToColumnType(Type type) {
    switch (type) {
      case INT8:
        return TiType.int8();
      case INT16:
        return TiType.int16();
      case INT32:
        return TiType.int32();
      case INT64:
        return TiType.int64();
      case FLOAT32:
        return TiType.floatType();
      case FLOAT64:
        return TiType.doubleType();
      case BOOLEAN:
        return TiType.bool();
      case STRING:
      case BYTES:
        return TiType.string();
    }
    return TiType.yson();
  }

  @Override
  public TableWriterManager getManager() {
    return new StaticTableWriterManager(config);
  }
}

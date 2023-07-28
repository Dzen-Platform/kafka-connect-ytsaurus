package ru.dzen.kafka.connect.ytsaurus.staticTables;

import com.google.common.collect.Sets;
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriter;
import ru.dzen.kafka.connect.ytsaurus.common.TableWriterManager;
import ru.dzen.kafka.connect.ytsaurus.common.Util;
import ru.dzen.kafka.connect.ytsaurus.staticTables.StaticTableWriterConfig.SchemaInferenceStrategy;
import ru.dzen.kafka.connect.ytsaurus.staticTables.schemaInference.InferFromFinalizedTable;
import ru.dzen.kafka.connect.ytsaurus.staticTables.schemaInference.InferFromFirstBatch;
import ru.dzen.kafka.connect.ytsaurus.staticTables.schemaInference.Disabled;
import ru.dzen.kafka.connect.ytsaurus.staticTables.schemaInference.SchemaInferenceStrategyImpl;
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
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

public class StaticTableWriter extends BaseTableWriter {

  private static final Logger log = LoggerFactory.getLogger(StaticTableWriter.class);

  public final StaticTableWriterConfig config;
  protected final SchemaManager schemaManager;

  public StaticTableWriter(StaticTableWriterConfig config) {
    super(config, new DocumentOffsetsManager(config.getOffsetsDirectory()));
    this.config = config;
    this.schemaManager = new SchemaManager(config.getSchemasDirectory());
  }

  @Override
  public TableWriterManager getManager() {
    return new StaticTableWriterManager(config);
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
  protected void writeRows(ApiServiceTransaction trx, Collection<SinkRecord> records)
      throws Exception {
    var tableRows = recordsToRows(records);
    var rows = tableRows.stream().map(x -> (YTreeMapNode) YTree.node(x.asMap()))
        .collect(Collectors.toList());

    var outputPath = getOutputTablePath(getNow());
    SchemaInferenceStrategyImpl schemaInferenceStrategy =
        getSchemaInferenceStrategyImpl(config.getSchemaInferenceStrategy());
    if (trx.existsNode(outputPath.toString()).get()) {
      trx.lockNode(outputPath.toString(), LockMode.Shared).get();
      if (trx.getNode(outputPath.allAttributes().toString()).get().mapNode().get("final")
          .isPresent()) {
        throw new RuntimeException("Tried to modify finalized table!");
      }
      schemaInferenceStrategy.init(trx, outputPath);
    } else {
      var createNodeBuilder = CreateNode.builder().setType(CypressNodeType.TABLE)
          .setPath(outputPath);
      schemaInferenceStrategy.update(trx, tableRows);
      schemaInferenceStrategy.getSchema(trx).ifPresent(s -> {
        createNodeBuilder.setAttributes(Map.of("schema", s.toYTree()));
      });
      for (var entry : config.getExtraTablesAttributes().entrySet()) {
        createNodeBuilder.addAttribute(entry.getKey(), entry.getValue());
      }
      trx.createNode(createNodeBuilder.build()).get();
    }

    Optional<TableSchema> inferredSchema = schemaInferenceStrategy.getSchema(trx);
    inferredSchema.ifPresent(tableSchema -> removeExtraColumns(rows, tableSchema));

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
        var accepted = inferredSchema.isPresent()
            ? writer.write(rows, inferredSchema.get())
            : writer.write(rows);

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
  }

  private SchemaInferenceStrategyImpl getSchemaInferenceStrategyImpl(
      SchemaInferenceStrategy schemaInferenceStrategy) {
    switch (schemaInferenceStrategy) {
      case DISABLED:
        return new Disabled();
      case INFER_FROM_FIRST_BATCH:
        return new InferFromFirstBatch();
      case INFER_FROM_FINALIZED_TABLE:
        return new InferFromFinalizedTable(schemaManager);
      default:
        throw new IllegalArgumentException(
            "SchemaInferenceStrategy '" + schemaInferenceStrategy.name() + "' is not supported");
    }
  }

  private void removeExtraColumns(List<YTreeMapNode> rows, TableSchema tableSchema) {
    Set<String> schemaColumns = new HashSet<>(tableSchema.getColumnNames());
    rows.forEach(r -> new HashSet<>(Sets.difference(r.keys(), schemaColumns)).forEach(r::remove));
  }
}

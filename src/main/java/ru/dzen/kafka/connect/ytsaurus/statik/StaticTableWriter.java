package ru.dzen.kafka.connect.ytsaurus.statik;

import java.time.Instant;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriter;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputTableSchemaType;
import ru.dzen.kafka.connect.ytsaurus.common.Util;
import ru.dzen.kafka.connect.ytsaurus.common.TableWriterManager;
import ru.dzen.kafka.connect.ytsaurus.common.UnstructuredTableSchema;
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

  protected static final Logger log = LoggerFactory.getLogger(StaticTableWriter.class);

  public final StaticTableWriterConfig config;
  private final TableSchema unstructuredTableSchema;

  public StaticTableWriter(StaticTableWriterConfig config) {
    super(config, new DocumentOffsetsManager(config.getOffsetsDirectory()));
    this.config = config;

    unstructuredTableSchema = UnstructuredTableSchema.createDataQueueTableSchema(
        config.getKeyOutputFormat(), config.getValueOutputFormat());

  }

  protected Instant getNow() throws Exception {
    return Util.retryWithBackoff(() -> client.generateTimestamps().get().getInstant(), 5, 100, 1000);
  }

  protected YPath getOutputTablePath(Instant now) {
    var rotationPeriod = config.getRotationPeriod();
    var dateOnly = rotationPeriod.getSeconds() % (60 * 60 * 25) == 0;
    var tableName = Util.formatDateTime(Util.floorByDuration(now, rotationPeriod), dateOnly);
    return YPath.simple(config.getOutputTablesDirectory() + "/" + tableName);
  }

  protected void writeRows(ApiServiceTransaction trx, Collection<SinkRecord> records)
      throws Exception {
    var outputPath = getOutputTablePath(getNow());
    if (trx.existsNode(outputPath.toString()).get()) {
      trx.lockNode(outputPath.toString(), LockMode.Shared).get();
      if (trx.getNode(outputPath.allAttributes().toString()).get().mapNode().get("final")
          .isPresent()) {
        throw new RuntimeException("Tried to modify finalized table!");
      }
    } else {
      trx.createNode(
          CreateNode.builder().setType(CypressNodeType.TABLE).setPath(outputPath).build()).get();
    }

    var mapNodesToWrite = recordsToUnstructuredRows(records);
    var rows = mapNodesToWrite.stream().map(x -> (YTreeMapNode) YTree.node(x))
        .collect(Collectors.toList());

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
  }

  @Override
  public TableWriterManager getManager() {
    return new StaticTableWriterManager(config);
  }
}

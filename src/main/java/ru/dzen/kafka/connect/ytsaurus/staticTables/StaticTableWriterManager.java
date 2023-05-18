package ru.dzen.kafka.connect.ytsaurus.staticTables;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.TableWriterManager;
import ru.dzen.kafka.connect.ytsaurus.common.Util;
import ru.dzen.kafka.connect.ytsaurus.staticTables.StaticTableWriterConfig.SchemaInferenceStrategy;
import tech.ytsaurus.client.operations.MergeSpec;
import tech.ytsaurus.client.operations.OperationStatus;
import tech.ytsaurus.client.request.ColumnFilter;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.ListNode;
import tech.ytsaurus.client.request.LockNode;
import tech.ytsaurus.client.request.MergeOperation;
import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.request.LockMode;
import tech.ytsaurus.ysontree.YTree;

public class StaticTableWriterManager extends StaticTableWriter implements TableWriterManager {

  private static final Logger log = LoggerFactory.getLogger(StaticTableWriterManager.class);

  private final ScheduledExecutorService scheduler;
  private ConnectorContext context;


  public StaticTableWriterManager(StaticTableWriterConfig config) {
    super(config);

    this.scheduler = Executors.newScheduledThreadPool(1);
  }

  private void rotate() {
    try {
      var now = getNow();
      var currentTablePath = getOutputTablePath(now);
      var retriesCount = 5;
      for (var i = 1; i <= retriesCount; i++) {
        log.info("[Try {} of {}] Freezing tables...", i, retriesCount);
        var numberOfTablesToFreeze = 0;
        try (var trx = this.createTransaction()) {
          var allTables = trx.listNode(
                  ListNode.builder()
                      .setPath(config.getOutputTablesDirectory())
                      .setAttributes(ColumnFilter.of("final"))
                      .build())
              .get().asList();
          for (var table : allTables) {
            if (table.getAttribute("final").isPresent() || config.getOutputTablesDirectory()
                .child(table.stringValue())
                .equals(currentTablePath)) {
              continue;
            }
            var tablePath = config.getOutputTablesDirectory().child(table.stringValue());
            log.info("Freezing table {}", tablePath);
            Util.waitAndLock(trx, LockNode.builder()
                .setPath(tablePath)
                .setWaitable(true)
                .setMode(LockMode.Exclusive)
                .build(), Duration.ofMinutes(2));

            var outputTableAttributes = new HashMap<>(Map.of(
                "final", YTree.booleanNode(true),
                "expiration_time", YTree.node(now.toEpochMilli() + config.getOutputTTL().toMillis())
            ));
            outputTableAttributes.putAll(config.getExtraTablesAttributes());
            if (config.getNeedToRunMerge()) {
              if (config.getSchemaInferenceStrategy()
                  .equals(SchemaInferenceStrategy.INFER_FROM_FINALIZED_TABLE)) {
                outputTableAttributes.put("schema", schemaManager.getPrevSchema(trx).toYTree());
              } else {
                outputTableAttributes.put("schema",
                    trx.getNode(tablePath.attribute("schema")).get());
              }
              log.info("Running merge for {}", tablePath);
              var oldTablePath = YPath.simple(tablePath + ".old");
              trx.moveNode(tablePath.toString(), oldTablePath.toString()).get();
              trx.createNode(
                  CreateNode.builder().setPath(tablePath).setAttributes(outputTableAttributes)
                      .setType(CypressNodeType.TABLE).build()).get();
              var mergeOperation = trx.startMerge(MergeOperation.builder().setSpec(
                  MergeSpec.builder().setInputTables(oldTablePath).setOutputTable(tablePath)
                      .setDataSizePerJob(DataSize.fromMegaBytes(config.getMergeDataSizePerJob()))
                      .setCombineChunks(true)
                      .build()).build()).get();
              log.info("Merge operation {} for {} started", mergeOperation.getId(),
                  tablePath);
              mergeOperation.watch().get();
              var mergeOperationStatus = mergeOperation.getStatus().get();
              if (mergeOperationStatus.equals(OperationStatus.COMPLETED)) {
                log.info("Completed merge for {}", tablePath);
              } else {
                var mergeOperationResult = mergeOperation.getResult().get();
                var errorMessage = "Merge for " + tablePath + " failed: " + mergeOperationResult;
                log.error(errorMessage);
                throw new Exception(errorMessage);
              }
              trx.removeNode(oldTablePath.toString()).get();
            } else {
              for (var entry : outputTableAttributes.entrySet()) {
                trx.setNode(tablePath.attribute(entry.getKey()).toString(), entry.getValue()).get();
              }
            }

            numberOfTablesToFreeze++;
          }
          trx.commit().get();
          log.info("Frozen {} tables", numberOfTablesToFreeze);
          break;
        } catch (Exception e) {
          log.warn("Can't freeze tables", e);
          if (i == retriesCount) {
            throw e;
          }
          try {
            Thread.sleep(5000);
          } catch (InterruptedException ex) {
            ex.printStackTrace();
          }
        }
      }
    } catch (Exception e) {
      this.context.raiseError(e);
    }
  }

  @Override
  public void start() throws RetriableException {
    // Perform any initialization or setup needed for StaticTablesQueueManager
    try {
      var createNodeBuilder = CreateNode.builder()
          .setPath(config.getOutputTablesDirectory())
          .setType(CypressNodeType.MAP)
          .setRecursive(true)
          .setIgnoreExisting(true);
      client.createNode(createNodeBuilder.build()).get();
      log.info("Created output tables directory {}", config.getOutputTablesDirectory());
      var chunkModeAttrPath = config.getOutputTablesDirectory().attribute("chunk_merger_mode");
      client.setNode(chunkModeAttrPath.toString(),
          YTree.node("auto")).get();
      log.info("Set {} to auto", chunkModeAttrPath);
    } catch (Exception e) {
      throw new RetriableException(e);
    }
    try {
      var createNodeBuilder = CreateNode.builder()
          .setPath(config.getOffsetsDirectory())
          .setType(CypressNodeType.MAP)
          .setRecursive(true)
          .setIgnoreExisting(true);
      client.createNode(createNodeBuilder.build()).get();
      log.info("Created offsets directory {}", config.getOffsetsDirectory());
    } catch (Exception e) {
      throw new RetriableException(e);
    }
    if (config.getSchemaInferenceStrategy()
        .equals(SchemaInferenceStrategy.INFER_FROM_FINALIZED_TABLE)) {
      try {
        var createNodeBuilder = CreateNode.builder()
            .setPath(config.getSchemasDirectory())
            .setType(CypressNodeType.MAP)
            .setRecursive(true)
            .setIgnoreExisting(true);
        client.createNode(createNodeBuilder.build()).get();
        log.info("Created schemas directory {}", config.getSchemasDirectory());
      } catch (Exception e) {
        throw new RetriableException(e);
      }
    }
    // Start the periodic task
    var periodMillis = config.getRotationPeriod().toMillis();
    var delayMillis = periodMillis - (Instant.now().toEpochMilli() % periodMillis);
    scheduler.scheduleAtFixedRate(this::rotate, delayMillis, periodMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    // Perform any cleanup, if necessary
    scheduler.shutdown();
  }

  @Override
  public void setContext(ConnectorContext context) {
    this.context = context;
  }
}
package ru.dzen.kafka.connect.ytsaurus.statik;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.RetriableException;
import ru.dzen.kafka.connect.ytsaurus.common.Util;
import ru.dzen.kafka.connect.ytsaurus.common.TableWriterManager;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.ColumnFilter;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.ListNode;
import tech.ytsaurus.client.request.LockNode;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.request.LockMode;
import tech.ytsaurus.ysontree.YTree;

public class StaticTableWriterManager extends StaticTableWriter implements TableWriterManager {

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
        log.info(String.format("[Try %d of %d] Freezing tables...", i, retriesCount));
        var trx = (ApiServiceTransaction) null;
        var numberOfTablesToFreeze = 0;
        try {
          trx = this.createTransaction();
          var allTables = trx.listNode(
                  ListNode.builder()
                      .setPath(config.getOutputTablesDirectory())
                      .setAttributes(ColumnFilter.of("final"))
                      .build())
              .get().asList();
          for (var table : allTables) {
            if (table.getAttribute("final").isPresent() || YPath.simple(
                    config.getOutputTablesDirectory() + "/" + table.stringValue())
                .equals(currentTablePath)) {
              continue;
            }
            var tablePath = YPath.simple(
                config.getOutputTablesDirectory() + "/" + table.stringValue());
            log.info(String.format("Freezing table %s", tablePath));
            Util.waitAndLock(trx, LockNode.builder()
                .setPath(tablePath)
                .setWaitable(true)
                .setMode(LockMode.Exclusive)
                .build(), Duration.ofMinutes(2));
            trx.setNode(tablePath.attribute("final").toString(), YTree.booleanNode(true)).get();
            trx.setNode(tablePath.attribute("expiration_time").toString(),
                YTree.node(now.toEpochMilli() + config.getOutputTTL().toMillis())).get();
            numberOfTablesToFreeze++;
          }
          trx.commit().get();
          log.info(String.format("Frozen %d tables", numberOfTablesToFreeze));
          break;
        } catch (Exception e) {
          log.warn("Can't freeze tables", e);
          if (trx != null) {
            trx.close();
          }
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
      log.info("Created output tables directory %s", config.getOutputTablesDirectory());
      var chunkModeAttrPath = config.getOutputTablesDirectory().attribute("chunk_merger_mode");
      client.setNode(chunkModeAttrPath.toString(),
          YTree.node("auto")).get();
      log.info("Set %s to auto", chunkModeAttrPath);
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
      log.info("Created offsets directory %s", config.getOffsetsDirectory());
    } catch (Exception e) {
      throw new RetriableException(e);
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
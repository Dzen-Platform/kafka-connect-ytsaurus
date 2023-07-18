package ru.dzen.kafka.connect.ytsaurus.common;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.AuthType;
import ru.dzen.kafka.connect.ytsaurus.table.TableRow;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.YTsaurusClientConfig;
import tech.ytsaurus.client.request.StartTransaction;

public abstract class BaseTableWriter {
  private static final Logger log = LoggerFactory.getLogger(BaseTableWriter.class);

  protected final YTsaurusClient client;
  protected final BaseOffsetsManager offsetsManager;
  protected final BaseTableWriterConfig config;
  private final TableRowMapper tableRowMapper;

  protected BaseTableWriter(BaseTableWriterConfig config, BaseOffsetsManager offsetsManager) {
    this.config = config;
    this.client = YTsaurusClient.builder()
        .setConfig(YTsaurusClientConfig.builder()
            .setTvmOnly(config.getAuthType().equals(AuthType.SERVICE_TICKET)).build())
        .setCluster(config.getYtCluster()).setAuth(config.getYtClientAuth()).build();
    this.offsetsManager = offsetsManager;
    this.tableRowMapper = new TableRowMapper(config);
  }

  protected ApiServiceTransaction createTransaction() throws Exception {
    return client.startTransaction(StartTransaction.master()).get();
  }

  public Map<TopicPartition, OffsetAndMetadata> getSafeToCommitOffsets(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) throws Exception {
    return offsetsManager.getCommittedOffsets(createTransaction(), currentOffsets.keySet())
        .entrySet()
        .stream()
        .filter(entry -> currentOffsets.containsKey(entry.getKey()))
        .map(entry -> {
          var topicPartition = entry.getKey();
          var committedOffset = entry.getValue();
          var currentOffset = currentOffsets.get(topicPartition);
          return currentOffset.offset() >= committedOffset.offset() ?
              Map.entry(topicPartition, committedOffset) :
              Map.entry(topicPartition, currentOffset);
        })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  protected List<TableRow> recordsToRows(Collection<SinkRecord> records) {
    return tableRowMapper.recordsToRows(records);
  }
  protected void writeRows(ApiServiceTransaction trx, Collection<SinkRecord> records)
      throws Exception {

  }

  protected void writeRows(ApiServiceTransaction trx, Collection<SinkRecord> records,
      Set<TopicPartition> topicPartitions)
      throws Exception {
    writeRows(trx, records);
  }

  public void writeBatch(Collection<SinkRecord> records) throws Exception {
    var startTime = System.currentTimeMillis();
    try (var trx = createTransaction()) {
      var maxOffsets = offsetsManager.getMaxOffsets(records);
      offsetsManager.lockPartitions(trx, maxOffsets.keySet());
      var committedOffsets = offsetsManager.getCommittedOffsets(trx, maxOffsets.keySet());
      var filteredRecords = offsetsManager.filterRecords(records, committedOffsets);
      if (filteredRecords.isEmpty()) {
        trx.close();
      } else {
        writeRows(trx, filteredRecords, maxOffsets.keySet());
        offsetsManager.writeOffsets(trx, maxOffsets);
        trx.commit().get();
      }
      var elapsed = Duration.ofMillis(System.currentTimeMillis() - startTime);
      log.info("Done processing batch in {}: {} total, {} written, {} skipped",
          Util.toHumanReadableDuration(elapsed), records.size(), filteredRecords.size(),
          records.size() - filteredRecords.size());
    } catch (Exception ex) {
      throw ex;
    }
  }

  public abstract TableWriterManager getManager();
}

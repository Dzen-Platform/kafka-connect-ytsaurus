package ru.dzen.kafka.connect.ytsaurus;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriter;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.DynTableWriter;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.DynTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.staticTables.StaticTableWriter;
import ru.dzen.kafka.connect.ytsaurus.staticTables.StaticTableWriterConfig;

public class YtTableSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(YtTableSinkTask.class);

  private BaseTableWriter producer;

  public YtTableSinkTask() {
  }

  @Override
  public String version() {
    return new YtTableSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    BaseTableWriterConfig config = new BaseTableWriterConfig(props);
    if (config.getOutputType() == BaseTableWriterConfig.OutputType.DYNAMIC_TABLE) {
      producer = new DynTableWriter(new DynTableWriterConfig(props));
    } else if (config.getOutputType() == BaseTableWriterConfig.OutputType.STATIC_TABLES) {
      producer = new StaticTableWriter(new StaticTableWriterConfig(props));
    }
    log.info("Started YtTableSinkTask");
  }

  @Override
  public void stop() {
    log.info("Stopped YtTableSinkTask");
  }


  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    try {
      producer.writeBatch(sinkRecords);
    } catch (Exception ex) {
      log.warn("Exception in put", ex);
      throw new RetriableException(ex);
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    try {
      return producer.getSafeToCommitOffsets(currentOffsets);
    } catch (Exception ex) {
      log.warn("Exception in preCommit", ex);
      return Collections.emptyMap();
    }
  }
}

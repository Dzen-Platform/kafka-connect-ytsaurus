package ru.dzen.kafka.connect.ytsaurus;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.common.TableWriterManager;
import ru.dzen.kafka.connect.ytsaurus.common.Util;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.DynTableWriter;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.DynTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.staticTables.StaticTableWriter;
import ru.dzen.kafka.connect.ytsaurus.staticTables.StaticTableWriterConfig;

public class YtTableSinkConnector extends SinkConnector {

  private static final Logger log = LoggerFactory.getLogger(YtTableSinkConnector.class);
  TableWriterManager manager;
  private Map<String, String> props;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    this.props = props;
    BaseTableWriterConfig config = new BaseTableWriterConfig(props);
    if (config.getOutputType() == BaseTableWriterConfig.OutputType.DYNAMIC_ORDERED_TABLES
        || config.getOutputType() == BaseTableWriterConfig.OutputType.DYNAMIC_SORTED_TABLES) {
      manager = new DynTableWriter(new DynTableWriterConfig(props)).getManager();
    } else if (config.getOutputType() == BaseTableWriterConfig.OutputType.STATIC_TABLES) {
      manager = new StaticTableWriter(new StaticTableWriterConfig(props)).getManager();
    }

    try {
      Util.retryWithBackoff(() -> {
        try {
          manager.start();
        } catch (Exception exc) {
          log.warn("Exception in start: ", exc);
          throw exc;
        }
      }, 10, 1000, 120000);
    } catch (Exception exc) {
      throw new ConnectException(exc);
    }

    log.info("Started YtTableSinkConnector");
  }

  @Override
  public void initialize(ConnectorContext ctx, List<Map<String, String>> taskConfigs) {
    super.initialize(ctx, taskConfigs);
    this.manager.setContext(ctx);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return YtTableSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return Stream.generate(() -> props)
        .limit(maxTasks)
        .collect(Collectors.toList());
  }

  @Override
  public void stop() {
    manager.stop();
    log.info("Stopped YtTableSinkConnector");
  }

  @Override
  public ConfigDef config() {
    return DynTableWriterConfig.CONFIG_DEF;
  }
}

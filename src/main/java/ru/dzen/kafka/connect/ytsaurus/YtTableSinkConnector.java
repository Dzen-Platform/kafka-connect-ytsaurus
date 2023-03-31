package ru.dzen.kafka.connect.ytsaurus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.common.TableWriterManager;
import ru.dzen.kafka.connect.ytsaurus.dynamic.DynTableWriter;
import ru.dzen.kafka.connect.ytsaurus.dynamic.DynTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.statik.StaticTableWriter;
import ru.dzen.kafka.connect.ytsaurus.statik.StaticTableWriterConfig;

public class YtTableSinkConnector extends SinkConnector {

  private static final Logger log = LoggerFactory.getLogger(YtTableSinkTask.class);
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
    if (config.getOutputType() == BaseTableWriterConfig.OutputType.DYNAMIC_TABLE) {
      manager = new DynTableWriter(new DynTableWriterConfig(props)).getManager();
    } else if (config.getOutputType() == BaseTableWriterConfig.OutputType.STATIC_TABLES) {
      manager = new StaticTableWriter(new StaticTableWriterConfig(props)).getManager();
    }
    manager.start();
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
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      configs.add(props);
    }
    return configs;
  }

  @Override
  public void stop() {
    manager.stop();
    log.info("Stopped YtTableSinkConnector");
  }


  private void validateConfig(BaseTableWriterConfig config) {
    if (config.getOutputType() == BaseTableWriterConfig.OutputType.DYNAMIC_TABLE) {
      if (config.getOutputTableSchemaType()
          != BaseTableWriterConfig.OutputTableSchemaType.UNSTRUCTURED) {
        throw new UnsupportedOperationException(
            "Only UNSTRUCTURED schema is supported for dynamic tables.");
      }
    } else if (config.getOutputType() == BaseTableWriterConfig.OutputType.STATIC_TABLES) {
      if (config.getOutputTableSchemaType() == BaseTableWriterConfig.OutputTableSchemaType.STRICT) {
        throw new UnsupportedOperationException(
            "STRICT schema is not supported for static tables.");
      }
    }
  }

  @Override
  public ConfigDef config() {
    return DynTableWriterConfig.CONFIG_DEF;
  }
}

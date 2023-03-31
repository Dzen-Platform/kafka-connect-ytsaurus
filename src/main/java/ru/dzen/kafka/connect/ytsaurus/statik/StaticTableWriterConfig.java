package ru.dzen.kafka.connect.ytsaurus.statik;

import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.common.Util;
import tech.ytsaurus.core.cypress.YPath;

public class StaticTableWriterConfig extends BaseTableWriterConfig {

  private static final String ROTATION_PERIOD = "yt.sink.static.rotation.period";
  private static final String OUTPUT_TABLES_DIRECTORY_POSTFIX = "yt.sink.static.tables.dir.postfix";

  public static ConfigDef CONFIG_DEF = new ConfigDef(BaseTableWriterConfig.CONFIG_DEF)
      .define(ROTATION_PERIOD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Rotation period")
      .define(OUTPUT_TABLES_DIRECTORY_POSTFIX, ConfigDef.Type.STRING, "output",
          ConfigDef.Importance.MEDIUM, "Output tables directory postfix");

  public StaticTableWriterConfig(Map<String, String> originals) throws ConnectException {
    super(CONFIG_DEF, originals);

    if (getOutputTableSchemaType().equals(OutputTableSchemaType.STRICT)) {
      throw new ConnectException("STRICT schema type is not supported by StaticTableWriter!");
    }
  }

  public YPath getOutputTablesDirectory() {
    return getOutputDirectory().child(getString(OUTPUT_TABLES_DIRECTORY_POSTFIX));
  }

  public YPath getOffsetsDirectory() {
    return getMetadataDirectory().child("offsets");
  }

  public Duration getRotationPeriod() {
    return Util.parseHumanReadableDuration(getString(ROTATION_PERIOD));
  }
}

package ru.dzen.kafka.connect.ytsaurus.common;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.typeinfo.TiType;

public class BaseTableWriterConfig extends AbstractConfig {

  public static final String YT_USER = "yt.connection.user";
  public static final String YT_TOKEN = "yt.connection.token";
  public static final String YT_CLUSTER = "yt.connection.cluster";
  public static final String OUTPUT_TYPE = "yt.sink.output.type";
  public static final String OUTPUT_TABLE_SCHEMA_TYPE = "yt.sink.output.table.schema.type";
  public static final String KEY_OUTPUT_FORMAT = "yt.sink.output.key.format";
  public static final String VALUE_OUTPUT_FORMAT = "yt.sink.output.value.format";
  public static final String OUTPUT_DIRECTORY = "yt.sink.output.directory";
  public static final String OUTPUT_TTL = "yt.sink.output.ttl";
  public static final String METADATA_DIRECTORY_NAME = "yt.sink.metadata.directory.name";

  public static ConfigDef CONFIG_DEF = new ConfigDef()
      .define(YT_USER, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
          "Username for the YT API authentication")
      .define(YT_TOKEN, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH,
          "Access token for the YT API authentication")
      .define(YT_CLUSTER, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
          "Identifier of the YT cluster to connect to")
      .define(OUTPUT_TYPE, ConfigDef.Type.STRING, OutputType.DYNAMIC_TABLE.name(),
          ValidUpperString.in(OutputType.DYNAMIC_TABLE.name(),
              OutputType.STATIC_TABLES.name()),
          ConfigDef.Importance.HIGH,
          "Specifies the output type: 'dynamic_table' for a sharded queue similar to Apache Kafka or 'static_tables' for separate time-based tables")
      .define(KEY_OUTPUT_FORMAT, ConfigDef.Type.STRING, OutputFormat.STRING.name(),
          ValidUpperString.in(OutputFormat.STRING.name(), OutputFormat.ANY.name()),
          ConfigDef.Importance.HIGH,
          "Determines the output format for keys: 'string' for plain string keys or 'any' for keys with no specific format")
      .define(VALUE_OUTPUT_FORMAT, ConfigDef.Type.STRING, OutputFormat.STRING.name(),
          ValidUpperString.in(OutputFormat.STRING.name(), OutputFormat.ANY.name()),
          ConfigDef.Importance.HIGH,
          "Determines the output format for values: 'string' for plain string values or 'any' for values with no specific format")
      .define(OUTPUT_TABLE_SCHEMA_TYPE, ConfigDef.Type.STRING,
          OutputTableSchemaType.UNSTRUCTURED.name(),
          ValidUpperString.in(OutputTableSchemaType.UNSTRUCTURED.name(),
              OutputTableSchemaType.STRICT.name(), OutputTableSchemaType.WEAK.name()),
          ConfigDef.Importance.HIGH,
          "Defines the schema type for output tables: 'unstructured' for schema-less tables, 'strict' for tables with a fixed schema, or 'weak' for tables with a flexible schema")
      .define(OUTPUT_DIRECTORY, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
          new YPathValidator(), ConfigDef.Importance.HIGH,
          "Specifies the directory path for storing the output data")
      .define(METADATA_DIRECTORY_NAME, ConfigDef.Type.STRING, "__connect_sink_metadata__",
          ConfigDef.Importance.MEDIUM, "Suffix for the metadata directory used by the system")
      .define(OUTPUT_TTL, ConfigDef.Type.STRING, "30d", new DurationValidator(),
          ConfigDef.Importance.MEDIUM,
          "Time-to-live (TTL) for output tables or rows, specified as a duration (e.g., '30d' for 30 days)");

  public BaseTableWriterConfig(ConfigDef configDef, Map<String, String> originals) {
    super(configDef, originals);
  }

  public BaseTableWriterConfig(Map<String, String> originals) {
    super(CONFIG_DEF, originals);
  }

  public String getYtUser() {
    return getString(YT_USER);
  }

  public String getYtToken() {
    return getPassword(YT_TOKEN).value();
  }

  public String getYtCluster() {
    return getString(YT_CLUSTER);
  }

  public OutputType getOutputType() {
    return OutputType.valueOf(getString(OUTPUT_TYPE).toUpperCase());
  }

  public OutputTableSchemaType getOutputTableSchemaType() {
    return OutputTableSchemaType.valueOf(getString(OUTPUT_TABLE_SCHEMA_TYPE).toUpperCase());
  }

  public OutputFormat getKeyOutputFormat() {
    return OutputFormat.valueOf(getString(KEY_OUTPUT_FORMAT).toUpperCase());
  }

  public OutputFormat getValueOutputFormat() {
    return OutputFormat.valueOf(getString(VALUE_OUTPUT_FORMAT).toUpperCase());
  }

  public YPath getOutputDirectory() {
    return YPath.simple(getString(OUTPUT_DIRECTORY));
  }

  public YPath getMetadataDirectory() {
    return getOutputDirectory().child(getString(METADATA_DIRECTORY_NAME));
  }

  public Duration getOutputTTL() {
    return Util.parseHumanReadableDuration(getString(OUTPUT_TTL));
  }

  public enum OutputType {
    DYNAMIC_TABLE,
    STATIC_TABLES
  }

  public enum OutputFormat {
    STRING,
    ANY;

    public TiType toTiType() {
      switch (this) {
        case STRING:
          return TiType.string();
        case ANY:
          return TiType.optional(TiType.yson());
        default:
          throw new IllegalArgumentException("Unsupported output format: " + this);
      }
    }
  }

  public enum OutputTableSchemaType {
    UNSTRUCTURED,
    STRICT,
    WEAK
  }

  public static class YPathValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(String name, Object value) {
      try {
        YPath.simple(value.toString());
      } catch (Exception ex) {
        throw new ConfigException(name, value, ex.toString());
      }
    }
  }

  public static class DurationValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(String name, Object value) {
      try {
        Util.parseHumanReadableDuration(value.toString());
      } catch (Exception ex) {
        throw new ConfigException(name, value, ex.toString());
      }
    }
  }

  public static class ValidUpperString implements ConfigDef.Validator {

    final List<String> validStrings;

    private ValidUpperString(List<String> validStrings) {
      this.validStrings = validStrings.stream().map(String::toUpperCase)
          .collect(Collectors.toList());
    }

    public static ValidUpperString in(String... validStrings) {
      return new ValidUpperString(Arrays.asList(validStrings));
    }

    @Override
    public void ensureValid(String name, Object o) {
      String s = ((String) o).toUpperCase();
      if (!validStrings.contains(s)) {
        throw new ConfigException(name, o,
            "String must be one of: " + Utils.join(validStrings, ", "));
      }
    }

    public String toString() {
      return "[" + Utils.join(
          validStrings.stream().map(String::toUpperCase).collect(Collectors.toList()), ", ") + "]";
    }
  }
}

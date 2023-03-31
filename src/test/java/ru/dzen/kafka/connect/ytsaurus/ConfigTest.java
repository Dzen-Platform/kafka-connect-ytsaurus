package ru.dzen.kafka.connect.ytsaurus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigValue;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.dynamic.DynTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.statik.StaticTableWriterConfig;

class ConfigTest {

  static final Map<String, String> correctBaseConfig = Map.ofEntries(
      Map.entry("yt.connection.user", "user"),
      Map.entry("yt.connection.token", "token"),
      Map.entry("yt.connection.cluster", "cluster"),
      Map.entry("yt.sink.output.type", "DYNAMIC_TABLE"),
      Map.entry("yt.sink.output.table.schema.type", "UNSTRUCTURED"),
      Map.entry("yt.sink.output.key.format", "ANY"),
      Map.entry("yt.sink.output.value.format", "ANY"),
      Map.entry("yt.sink.output.directory", "//home/user/output"),
      Map.entry("yt.sink.output.ttl", "30m"),
      Map.entry("yt.sink.metadata.directory.postfix", "_metadata")
  );
  static final Map<String, String> correctDynamicConfig = new HashMap<>(correctBaseConfig);
  static final Map<String, String> correctStaticConfig = new HashMap<>(correctBaseConfig);

  static {
    correctDynamicConfig.put("yt.sink.output.type", "DYNAMIC_TABLE");
    correctDynamicConfig.put("yt.sink.dynamic.queue.postfix", "queue");
    correctDynamicConfig.put("yt.sink.dynamic.queue.auto.create", "true");
    correctDynamicConfig.put("yt.sink.dynamic.queue.tablet.count", "10");
  }

  static {
    correctStaticConfig.put("yt.sink.output.type", "STATIC_TABLES");
    correctStaticConfig.put("yt.sink.static.rotation.period", "10m");
    correctStaticConfig.put("yt.sink.static.tables.dir.postfix", "output");
  }

  @org.junit.jupiter.api.Test
  void baseConfig() {
    List<ConfigValue> configValueList = BaseTableWriterConfig.CONFIG_DEF.validate(
        correctBaseConfig);
    for (ConfigValue configValue : configValueList) {
      assertEquals(0, configValue.errorMessages().size());
    }
  }

  @org.junit.jupiter.api.Test
  void dynamicConfig() {
    List<ConfigValue> configValueList = DynTableWriterConfig.CONFIG_DEF.validate(
        correctDynamicConfig);
    for (ConfigValue configValue : configValueList) {
      assertEquals(0, configValue.errorMessages().size());
    }
  }

  @org.junit.jupiter.api.Test
  void staticConfig() {
    List<ConfigValue> configValueList = StaticTableWriterConfig.CONFIG_DEF.validate(
        correctStaticConfig);
    for (ConfigValue configValue : configValueList) {
      assertEquals(0, configValue.errorMessages().size());
    }
  }

  @org.junit.jupiter.api.Test
  void invalidTTL() {
    Map<String, String> incorrectConfig = new HashMap<>(correctBaseConfig);
    incorrectConfig.put("yt.sink.output.ttl", "123123");
    List<ConfigValue> configValueList = DynTableWriterConfig.CONFIG_DEF.validate(incorrectConfig);
    for (ConfigValue configValue : configValueList) {
      if (configValue.name() == "yt.sink.output.ttl") {
        assertEquals(1, configValue.errorMessages().size());
        assertTrue(
            configValue.errorMessages().get(0).contains("Text cannot be parsed to a Duration"));
      }
    }
  }
}
package ru.dzen.kafka.connect.ytsaurus.staticTables;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.common.Util;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

public class StaticTableWriterConfig extends BaseTableWriterConfig {

  private static final String ROTATION_PERIOD = "yt.sink.static.rotation.period";
  private static final String OUTPUT_TABLES_DIRECTORY_POSTFIX = "yt.sink.static.tables.dir.postfix";
  private static final String MERGE_CHUNKS = "yt.sink.static.merge.chunks";
  private static final String MERGE_DATA_SIZE_PER_JOB = "yt.sink.static.merge.data.size.per.job";
  private static final String SCHEMA_INFERENCE_STRATEGY = "yt.sink.static.tables.schema.inference.strategy";
  private static final String COMPRESSION_CODEC = "yt.sink.static.tables.compression.codec";
  private static final String OPTIMIZE_FOR = "yt.sink.static.tables.optimize.for";
  private static final String REPLICATION_FACTOR = "yt.sink.static.tables.replication.factor";
  private static final String ERASURE_CODEC = "yt.sink.static.tables.erasure.codec";

  public static ConfigDef CONFIG_DEF = new ConfigDef(BaseTableWriterConfig.CONFIG_DEF)
      .define(ROTATION_PERIOD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Rotation period")
      .define(OUTPUT_TABLES_DIRECTORY_POSTFIX, ConfigDef.Type.STRING, "output",
          ConfigDef.Importance.MEDIUM, "Output tables directory postfix")
      .define(COMPRESSION_CODEC, ConfigDef.Type.STRING, "zstd_3", ConfigDef.Importance.MEDIUM,
          "Compression codec of the output tables.")
      .define(OPTIMIZE_FOR, ConfigDef.Type.STRING, "lookup",
          ConfigDef.ValidString.in("lookup", "scan"), ConfigDef.Importance.MEDIUM,
          "Specifies the storage optimization strategy for the table. Choose 'lookup' for row-based table storage optimized for point lookups, or 'scan' for column-based table storage optimized for scans and aggregations.")
      .define(REPLICATION_FACTOR, ConfigDef.Type.INT, null,
          ConfigDef.Importance.MEDIUM, "The replication factor of the output tables.")
      .define(ERASURE_CODEC, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
          "Erasure coding codec of the output tables.")
      .define(MERGE_CHUNKS, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
          "Activate the consolidation of chunks during the table rotation process.")
      .define(MERGE_DATA_SIZE_PER_JOB, ConfigDef.Type.INT, 100, ConfigDef.Importance.MEDIUM,
          "Maximum size of data to be merged per job in MB.")
      .define(SCHEMA_INFERENCE_STRATEGY, ConfigDef.Type.STRING,
          SchemaInferenceStrategy.DISABLED.name(),
          ValidUpperString.in(SchemaInferenceStrategy.DISABLED.name(),
              SchemaInferenceStrategy.INFER_FROM_FIRST_BATCH.name(),
              SchemaInferenceStrategy.INFER_FROM_FINALIZED_TABLE.name()),
          ConfigDef.Importance.HIGH,
          "The strategy for inferring the schema of the output tables. Valid options are DISABLED, INFER_FROM_FIRST_BATCH, and INFER_FROM_FINALIZED_TABLE.\n"
              +
              "DISABLED means that the schema will not be inferred at all, and the output tables will have a weak schema that only includes the column names.\n"
              +
              "INFER_FROM_FIRST_BATCH means that the table schema will be created from the first batch of data, and will not change after the table is created.\n"
              +
              "INFER_FROM_FINALIZED_TABLE means that the weak schema will be used during the writing of rows, and after rotation, the finalized table will be re-merged with the schema based on all rows of the table.");

  public StaticTableWriterConfig(Map<String, String> originals) throws ConnectException {
    super(CONFIG_DEF, originals);

    if (getOutputTableSchemaType().equals(OutputTableSchemaType.STRICT)) {
      if (!getValueOutputFormat().equals(OutputFormat.ANY)) {
        throw new ConnectException(
            "When using the STRICT table schema type, the value format must be set to ANY!");
      }
    } else if (!getSchemaInferenceStrategy().equals(SchemaInferenceStrategy.DISABLED)) {
      throw new ConnectException(
          "Schema inference strategy could be enabled only when using STRICT schema type");
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

  public YPath getSchemasDirectory() {
    return getMetadataDirectory().child("schemas");
  }

  public SchemaInferenceStrategy getSchemaInferenceStrategy() {
    String strategyName = getString(SCHEMA_INFERENCE_STRATEGY);
    return SchemaInferenceStrategy.valueOf(strategyName);
  }

  public boolean getNeedToRunMerge() {
    return getBoolean(MERGE_CHUNKS) || getSchemaInferenceStrategy().equals(
        SchemaInferenceStrategy.INFER_FROM_FINALIZED_TABLE);
  }

  public int getMergeDataSizePerJob() {
    return getInt(MERGE_DATA_SIZE_PER_JOB);
  }

  public Map<String, YTreeNode> getExtraTablesAttributes() {
    Map<String, YTreeNode> extraAttributes = new HashMap<>();
    extraAttributes.put("compression_codec", YTree.node(getString(COMPRESSION_CODEC)));
    extraAttributes.put("optimize_for", YTree.node(getString(OPTIMIZE_FOR)));

    Integer replicationFactor = getInt(REPLICATION_FACTOR);
    if (replicationFactor != null) {
      extraAttributes.put("replication_factor", YTree.node(replicationFactor));
    }

    String erasureCodec = getString(ERASURE_CODEC);
    if (erasureCodec != null) {
      extraAttributes.put("erasure_codec", YTree.node(erasureCodec));
    }

    return extraAttributes;
  }

  public enum SchemaInferenceStrategy {
    DISABLED,
    INFER_FROM_FIRST_BATCH,
    INFER_FROM_FINALIZED_TABLE
  }

}

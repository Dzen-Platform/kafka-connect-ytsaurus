package ru.dzen.kafka.connect.ytsaurus.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputTableSchemaType;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.YtClientConfiguration;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

public abstract class BaseTableWriter {

  protected static final Logger log = LoggerFactory.getLogger(BaseTableWriter.class);
  protected static final ObjectMapper objectMapper = new ObjectMapper();
  private static final JsonConverter JSON_CONVERTER;

  static {
    JSON_CONVERTER = new JsonConverter();
    JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", "false"), false);
  }

  protected final YTsaurusClient client;
  protected final BaseOffsetsManager offsetsManager;
  protected final BaseTableWriterConfig config;

  protected BaseTableWriter(BaseTableWriterConfig config, BaseOffsetsManager offsetsManager) {
    this.config = config;
    this.client = YTsaurusClient.builder()
        .setYtClientConfiguration(new YtClientConfiguration.Builder().build())
        .setCluster(config.getYtCluster()).setAuth(
            YTsaurusClientAuth.builder()
                .setUser(config.getYtUser())
                .setToken(config.getYtToken())
                .build()).build();
    this.offsetsManager = offsetsManager;
  }

  protected ApiServiceTransaction createTransaction() throws Exception {
    return client.startTransaction(StartTransaction.master()).get();
  }

  public Map<TopicPartition, OffsetAndMetadata> getSafeToCommitOffsets(
      Map<TopicPartition, OffsetAndMetadata> unsafeOffsets) throws Exception {
    var trx = createTransaction();
    var res = new HashMap<TopicPartition, OffsetAndMetadata>();
    for (var entry : offsetsManager.getPrevOffsets(trx,
        unsafeOffsets.keySet()).entrySet()) {
      if (unsafeOffsets.containsKey(entry.getKey())) {
        if (unsafeOffsets.get(entry.getKey()).offset() >= entry.getValue().offset()) {
          res.put(entry.getKey(), entry.getValue());
        } else {
          // commit older offset if not yet consumed safe offset
          res.put(entry.getKey(), unsafeOffsets.get(entry.getKey()));
        }
      }
    }
    return res;
  }

  protected Object convertRecordKey(SinkRecord record) throws Exception {
    if (record.key() == null) {
      return JsonNodeFactory.instance.nullNode();
    }
    if (record.key() instanceof String) {
      return record.key();
    }

    byte[] jsonBytes = JSON_CONVERTER.fromConnectData(record.topic(), record.keySchema(),
        record.key());
    var jsonString = new String(jsonBytes, StandardCharsets.UTF_8);

    var objectMapper = new ObjectMapper();
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    return jsonNode;
  }

  protected YTreeNode convertRecordKeyToNode(SinkRecord record) throws Exception {
    var recordKey = convertRecordKey(record);

    if (config.getKeyOutputFormat() == BaseTableWriterConfig.OutputFormat.STRING
        && !(recordKey instanceof String)) {
      recordKey = objectMapper.writeValueAsString(recordKey);
    } else if (!(recordKey instanceof String)) {
      recordKey = Util.convertJsonNodeToYTree((JsonNode) recordKey);
    }

    return YTree.node(recordKey);
  }

  protected Object convertRecordValue(SinkRecord record) throws Exception {
    if (record.value() == null) {
      return JsonNodeFactory.instance.nullNode();
    }
    if (record.value() instanceof String) {
      return record.value();
    }

    byte[] jsonBytes = JSON_CONVERTER.fromConnectData(record.topic(), record.valueSchema(),
        record.value());
    var jsonString = new String(jsonBytes, StandardCharsets.UTF_8);

    var objectMapper = new ObjectMapper();
    JsonNode jsonNode = objectMapper.readTree(jsonString);

    return jsonNode;
  }

  protected YTreeNode convertRecordValueToNode(SinkRecord record) throws Exception {
    var recordValue = convertRecordValue(record);
    if (config.getValueOutputFormat() == BaseTableWriterConfig.OutputFormat.STRING
        && !(recordValue instanceof String)) {
      recordValue = objectMapper.writeValueAsString(recordValue);
    } else if (!(recordValue instanceof String)) {
      recordValue = Util.convertJsonNodeToYTree((JsonNode) recordValue);
    }

    return YTree.node(recordValue);
  }

  protected List<Map<String, YTreeNode>> recordsToRows(Collection<SinkRecord> records) {
    var mapNodesToWrite = new ArrayList<Map<String, YTreeNode>>();
    for (SinkRecord record : records) {
      var headersBuilder = YTree.builder().beginList();
      for (Header header : record.headers()) {
        headersBuilder.value(
            YTree.builder().beginList().value(header.key()).value(header.value().toString())
                .buildList());
      }
      YTreeNode recordKeyNode;
      try {
        recordKeyNode = convertRecordKeyToNode(record);
      } catch (Exception e) {
        log.error("Exception in convertRecordKeyToNode:", e);
        throw new DataException(e);
      }
      YTreeNode recordValueNode;
      try {
        recordValueNode = convertRecordValueToNode(record);
      } catch (Exception e) {
        log.error("Exception in convertRecordValueToNode:", e);
        throw new DataException(e);
      }

      Map<String, YTreeNode> rowMap = new HashMap<>();
      if (config.getOutputTableSchemaType().equals(OutputTableSchemaType.UNSTRUCTURED)) {
        rowMap.put(UnstructuredTableSchema.EColumn.DATA.name, recordValueNode);
      } else {
        if (!recordValueNode.isMapNode()) {
          throw new DataException(String.format("Record value is not a map: %s", recordValueNode));
        }
        rowMap = recordValueNode.asMap();
      }

      rowMap.put(UnstructuredTableSchema.EColumn.KEY.name, recordKeyNode);
      rowMap.put(UnstructuredTableSchema.EColumn.TOPIC.name, YTree.stringNode(record.topic()));
      rowMap.put(UnstructuredTableSchema.EColumn.PARTITION.name,
          YTree.unsignedLongNode(record.kafkaPartition()));
      rowMap.put(UnstructuredTableSchema.EColumn.OFFSET.name,
          YTree.unsignedLongNode(record.kafkaOffset()));
      rowMap.put(UnstructuredTableSchema.EColumn.TIMESTAMP.name,
          YTree.unsignedLongNode(System.currentTimeMillis()));
      rowMap.put(UnstructuredTableSchema.EColumn.HEADERS.name, headersBuilder.buildList());
      mapNodesToWrite.add(rowMap);
    }
    return mapNodesToWrite;
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
    var trx = createTransaction();
    try {
      var maxOffsets = offsetsManager.getMaxOffsets(records);
      offsetsManager.lockPartitions(trx, maxOffsets.keySet());
      var prevOffsets = offsetsManager.getPrevOffsets(trx,
          maxOffsets.keySet());
      var filteredRecords = offsetsManager.filterRecords(records, prevOffsets);
      if (filteredRecords.isEmpty()) {
        trx.close();
      } else {
        writeRows(trx, filteredRecords, maxOffsets.keySet());
        offsetsManager.writeOffsets(trx, maxOffsets);
        trx.commit().get();
      }
      var elapsed = Duration.ofMillis(System.currentTimeMillis() - startTime);
      log.info(String.format("Done processing batch in %s: %d total, %d written, %d skipped",
          Util.toHumanReadableDuration(elapsed), records.size(), filteredRecords.size(),
          records.size() - filteredRecords.size()));
    } catch (Exception ex) {
      trx.close();
      throw ex;
    }
  }

  public abstract TableWriterManager getManager();
}

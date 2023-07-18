package ru.dzen.kafka.connect.ytsaurus.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputTableSchemaType;
import ru.dzen.kafka.connect.ytsaurus.table.TableRow;
import ru.dzen.kafka.connect.ytsaurus.table.TableRow.Builder;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeListNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * @author pbk-vitaliy
 */
class TableRowMapper {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Logger log = LoggerFactory.getLogger(TableRowMapper.class);
  private static final JsonConverter jsonConverter;

  static {
    jsonConverter = new JsonConverter();
    jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
  }

  private final BaseTableWriterConfig config;

  TableRowMapper(BaseTableWriterConfig config) {
    this.config = config;
  }

  protected List<TableRow> recordsToRows(Collection<SinkRecord> records) {
    var tableRows = new ArrayList<TableRow>();
    for (SinkRecord record : records) {
      Builder rowBuilder = TableRow.builder();
      try {
        addRecordKeyColumns(rowBuilder, record);
      } catch (Exception e) {
        log.error("Exception in addRecordKeyColumns:", e);
        throw new DataException(e);
      }
      try {
        addRecordValueColumns(rowBuilder, record);
      } catch (Exception e) {
        log.error("Exception in addRecordValueColumns:", e);
        throw new DataException(e);
      }

      rowBuilder.addValueColumn(
          UnstructuredTableSchema.EColumn.TOPIC.name,
          TiType.string(),
          YTree.stringNode(record.topic())
      );
      rowBuilder.addValueColumn(
          UnstructuredTableSchema.EColumn.PARTITION.name,
          TiType.uint64(),
          YTree.unsignedLongNode(record.kafkaPartition())
      );
      rowBuilder.addValueColumn(
          UnstructuredTableSchema.EColumn.OFFSET.name,
          TiType.uint64(),
          YTree.unsignedLongNode(record.kafkaOffset())
      );
      rowBuilder.addValueColumn(
          UnstructuredTableSchema.EColumn.TIMESTAMP.name,
          TiType.uint64(),
          YTree.unsignedLongNode(System.currentTimeMillis())
      );
      rowBuilder.addValueColumn(
          UnstructuredTableSchema.EColumn.HEADERS.name,
          TiType.list(TiType.list(TiType.string())),
          buildHeaders(record)
      );
      tableRows.add(rowBuilder.build());
    }
    return tableRows;
  }

  private void addRecordKeyColumns(TableRow.Builder rowBuilder, SinkRecord record)
      throws Exception {
    if (record.key() == null) {
      rowBuilder.addKeyColumn(
          UnstructuredTableSchema.EColumn.KEY.name,
          TiType.nullType(),
          YTree.nullNode()
      );
      return;
    }
    if (record.key() instanceof String) {
      rowBuilder.addKeyColumn(
          UnstructuredTableSchema.EColumn.KEY.name,
          TiType.string(),
          YTree.stringNode((String) record.key())
      );
      return;
    }

    byte[] jsonBytes = jsonConverter.fromConnectData(record.topic(), record.keySchema(),
        record.key());
    var jsonString = new String(jsonBytes, StandardCharsets.UTF_8);
    if (config.getKeyOutputFormat() == BaseTableWriterConfig.OutputFormat.STRING) {
      rowBuilder.addKeyColumn(
          UnstructuredTableSchema.EColumn.KEY.name,
          TiType.string(),
          YTree.stringNode(jsonString)
      );
      return;
    }

    JsonNode jsonNode = objectMapper.readTree(jsonString);
    YTreeNode yTreeNode = Util.convertJsonNodeToYTree(jsonNode);
    rowBuilder.addKeyColumn(
        UnstructuredTableSchema.EColumn.KEY.name,
        Util.getTypeOfNode(yTreeNode),
        yTreeNode
    );
  }

  private void addRecordValueColumns(TableRow.Builder rowBuilder, SinkRecord record)
      throws Exception {
    if (record.value() == null) {
      if (config.getOutputTableSchemaType().equals(OutputTableSchemaType.UNSTRUCTURED)) {
        rowBuilder.addValueColumn(
            UnstructuredTableSchema.EColumn.DATA.name,
            TiType.nullType(),
            YTree.nullNode()
        );
      }
      return;
    }
    if (record.value() instanceof String) {
      if (!config.getOutputTableSchemaType().equals(OutputTableSchemaType.UNSTRUCTURED)) {
        throw new DataException("String record values not supported for schema type: " +
            config.getOutputTableSchemaType());
      }
      rowBuilder.addValueColumn(
          UnstructuredTableSchema.EColumn.DATA.name,
          TiType.string(),
          YTree.stringNode((String) record.value())
      );
      return;
    }
    byte[] valueBytes = jsonConverter.fromConnectData(record.topic(), record.valueSchema(),
        record.value());
    var valueString = new String(valueBytes, StandardCharsets.UTF_8);
    if (config.getValueOutputFormat() == BaseTableWriterConfig.OutputFormat.STRING) {
      if (!config.getOutputTableSchemaType().equals(OutputTableSchemaType.UNSTRUCTURED)) {
        throw new DataException("String record values not supported for schema type: " +
            config.getOutputTableSchemaType());
      }
      rowBuilder.addValueColumn(
          UnstructuredTableSchema.EColumn.DATA.name,
          TiType.string(),
          YTree.stringNode(valueString)
      );
      return;
    }

    JsonNode valueJson = objectMapper.readTree(valueString);
    YTreeNode valueYTreeNode = Util.convertJsonNodeToYTree(valueJson);
    if (config.getOutputTableSchemaType().equals(OutputTableSchemaType.UNSTRUCTURED)) {
      rowBuilder.addValueColumn(
          UnstructuredTableSchema.EColumn.DATA.name,
          Util.getTypeOfNode(valueYTreeNode),
          valueYTreeNode
      );
    } else {
      if (!valueYTreeNode.isMapNode()) {
        throw new DataException("Record value is not a map: " + valueYTreeNode);
      }
      valueYTreeNode.asMap().forEach(
          (col, colVal) -> rowBuilder.addValueColumn(col, Util.getTypeOfNode(colVal), colVal)
      );
    }
  }

  private static YTreeListNode buildHeaders(SinkRecord record) {
    var headersBuilder = YTree.builder().beginList();
    for (Header header : record.headers()) {
      headersBuilder.value(
          YTree.builder().beginList().value(header.key()).value(header.value().toString())
              .buildList());
    }
    return headersBuilder.buildList();
  }
}

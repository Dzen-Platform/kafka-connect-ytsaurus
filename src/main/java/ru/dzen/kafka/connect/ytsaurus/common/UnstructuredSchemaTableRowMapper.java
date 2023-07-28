package ru.dzen.kafka.connect.ytsaurus.common;

import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputFormat;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputType;
import ru.dzen.kafka.connect.ytsaurus.serialization.SerializationUtils;
import ru.dzen.kafka.connect.ytsaurus.serialization.TypedNode;
import ru.dzen.kafka.connect.ytsaurus.common.TableRow.Builder;
import tech.ytsaurus.core.tables.ColumnSortOrder;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;

/**
 * @author pbk-vitaliy
 */
public class UnstructuredSchemaTableRowMapper implements TableRowMapper, Configurable {
  private static final Logger log = LoggerFactory.getLogger(UnstructuredSchemaTableRowMapper.class);

  private OutputFormat keyOutputFormat = OutputFormat.STRING;
  private OutputFormat valueOutputFormat = OutputFormat.STRING;
  private ColumnSortOrder keyColumnsSortOrder;

  @Override
  public void configure(Map<String, ?> props) {
    BaseTableWriterConfig config = new BaseTableWriterConfig(props);
    keyOutputFormat = config.getKeyOutputFormat();
    valueOutputFormat = config.getValueOutputFormat();
    if (config.getOutputType() == OutputType.DYNAMIC_SORTED_TABLES) {
      keyColumnsSortOrder = ColumnSortOrder.ASCENDING;
    }
  }

  public TableRow recordToRow(SinkRecord record) {
    TypedNode typedKeyNode;
    try {
      typedKeyNode = SerializationUtils.serializeToSingleColumn(
          record.keySchema(), record.key(), keyOutputFormat);
    } catch (Exception e) {
      log.error("Exception in key serialization:", e);
      throw new DataException(e);
    }
    TypedNode typedValueNode;
    try {
      typedValueNode = SerializationUtils.serializeToSingleColumn(
          record.valueSchema(), record.value(), valueOutputFormat);
    } catch (Exception e) {
      log.error("Exception in value serialization:", e);
      throw new DataException(e);
    }
    TypedNode typedHeadersNode;
    try {
      typedHeadersNode = SerializationUtils.serializeRecordHeaders(record.headers());
    } catch (Exception e) {
      log.error("Exception in headers serialization:", e);
      throw new DataException(e);
    }

    Builder rowBuilder = TableRow.builder();
    rowBuilder.setKeyColumnsSortOrder(keyColumnsSortOrder);
    rowBuilder.addKeyColumn(
        UnstructuredTableSchema.EColumn.KEY.name,
        typedKeyNode.getType(),
        typedKeyNode.getValue()
    );
    rowBuilder.addValueColumn(
        UnstructuredTableSchema.EColumn.DATA.name,
        typedValueNode.getType(),
        typedValueNode.getValue()
    );
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
        typedHeadersNode.getType(),
        typedHeadersNode.getValue()
    );
    return rowBuilder.build();
  }
}

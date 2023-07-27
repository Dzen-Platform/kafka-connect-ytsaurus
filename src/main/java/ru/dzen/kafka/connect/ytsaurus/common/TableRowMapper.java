package ru.dzen.kafka.connect.ytsaurus.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;
import ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations.TableRow;

/**
 * @author pbk-vitaliy
 */
public interface TableRowMapper {

  TableRow recordToRow(SinkRecord record);

  default List<TableRow> recordsToRows(Collection<SinkRecord> records) {
    var tableRows = new ArrayList<TableRow>();
    for (SinkRecord record : records) {
      tableRows.add(recordToRow(record));
    }
    return tableRows;
  }
}

package ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations;

import java.util.List;
import ru.dzen.kafka.connect.ytsaurus.common.TableRow;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.client.request.ModifyRowsRequest.Builder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;

/**
 * @author pbk-vitaliy
 */
public class DyntableRowsDelete implements TableOperation {
  private final YPath tablePath;
  private final List<TableRow> deletedRows;

  public DyntableRowsDelete(YPath tablePath, List<TableRow> deletedRows) {
    this.tablePath = tablePath;
    this.deletedRows = deletedRows;
  }

  @Override
  public void execute(ApiServiceTransaction tx) {
    if (deletedRows.isEmpty()) {
      return;
    }
    TableSchema tableSchema = deletedRows.get(0).getSchema();
    Builder requestBuilder = ModifyRowsRequest.builder()
        .setPath(tablePath.toString())
        .setSchema(tableSchema);
    deletedRows.forEach(row -> requestBuilder.addDelete(row.getKey()));
    tx.modifyRows(requestBuilder.build()).join();
  }
}

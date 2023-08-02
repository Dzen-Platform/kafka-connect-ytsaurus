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
public class DyntableRowsUpdate implements TableOperation {
  private final YPath tablePath;
  private final List<TableRow> updatedRows;

  public DyntableRowsUpdate(YPath tablePath, List<TableRow> updatedRows) {
    this.tablePath = tablePath;
    this.updatedRows = updatedRows;
  }

  @Override
  public void execute(ApiServiceTransaction tx) {
    if (updatedRows.isEmpty()) {
      return;
    }
    TableSchema tableSchema = updatedRows.get(0).getSchema();
    Builder requestBuilder = ModifyRowsRequest.builder()
        .setPath(tablePath.toString())
        .setSchema(tableSchema);
    updatedRows.forEach(row -> requestBuilder.addUpdate(row.asMap()));
    tx.modifyRows(requestBuilder.build()).join();
  }
}

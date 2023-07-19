package ru.dzen.kafka.connect.ytsaurus.table;

import java.util.List;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.client.request.ModifyRowsRequest.Builder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;

/**
 * @author pbk-vitaliy
 */
public class DyntableRowsInsert implements Update {
  private final YPath tablePath;
  private final List<TableRow> rowsToInsert;

  public DyntableRowsInsert(YPath tablePath, List<TableRow> rowsToInsert) {
    this.tablePath = tablePath;
    this.rowsToInsert = rowsToInsert;
  }

  @Override
  public void execute(ApiServiceTransaction tx) {
    if (rowsToInsert.isEmpty()) {
      return;
    }
    TableSchema tableSchema = rowsToInsert.get(0).getSchema();
    Builder requestBuilder = ModifyRowsRequest.builder()
        .setPath(tablePath.toString())
        .setSchema(tableSchema);
    rowsToInsert.forEach(row -> requestBuilder.addInsert(row.asMap()));
    tx.modifyRows(requestBuilder.build()).join();
  }
}

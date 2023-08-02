package ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations;

import tech.ytsaurus.client.ApiServiceTransaction;

/**
 * @author pbk-vitaliy
 */
public interface TableOperation {
  void execute(ApiServiceTransaction tx);
}

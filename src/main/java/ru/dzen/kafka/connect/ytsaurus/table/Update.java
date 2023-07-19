package ru.dzen.kafka.connect.ytsaurus.table;

import tech.ytsaurus.client.ApiServiceTransaction;

/**
 * @author pbk-vitaliy
 */
public interface Update {
  void execute(ApiServiceTransaction tx);
}

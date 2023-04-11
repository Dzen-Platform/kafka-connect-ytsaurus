package ru.dzen.kafka.connect.ytsaurus.common;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.RetriableException;

public interface TableWriterManager {

  void start() throws RetriableException;

  void stop();

  default void setContext(ConnectorContext context) {
  }
}

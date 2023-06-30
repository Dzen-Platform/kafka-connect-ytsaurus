package ru.dzen.kafka.connect.ytsaurus.integration;

import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

/**
 * @author pbk-vitaliy
 */
public abstract class BaseConnectorIntegrationTest {

  protected EmbeddedConnectCluster connect;

  protected void startConnect() {
    connect = new EmbeddedConnectCluster.Builder()
        .name("ytsaurus-sink-connect-cluster")
        .build();
    connect.start();
  }

  protected void stopConnect() {
    if (connect != null) {
      connect.stop();
    }
  }
}

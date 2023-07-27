package ru.dzen.kafka.connect.ytsaurus.integration;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.dzen.kafka.connect.ytsaurus.YtTableSinkConnector;

/**
 * @author pbk-vitaliy
 */
public abstract class BaseConnectorIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIntegrationTest.class);

  protected EmbeddedConnectCluster connect;
  protected Map<String, String> sinkConnectorProps = new HashMap<>();

  private Admin kafkaAdmin;

  protected void startConnect() {
    connect = new EmbeddedConnectCluster.Builder()
        .name("ytsaurus-sink-connect-cluster")
        .workerProps(defaultWorkerProps())
        .build();
    connect.start();
    kafkaAdmin = connect.kafka().createAdminClient();
  }

  protected void stopConnect() {
    if (connect != null) {
      connect.stop();
    }
  }

  protected Map<String, String> baseSinkConnectorProps() {
    Map<String, String> props = new HashMap<>();
    props.put(SinkConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put(SinkConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(SinkConnectorConfig.CONNECTOR_CLASS_CONFIG, YtTableSinkConnector.class.getName());
    props.put(SinkConnectorConfig.TASKS_MAX_CONFIG, "1");
    props.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "50");
    return props;
  }

  protected void awaitConnectorIsStarted(String connectorName) {
    await("Starting connector " + connectorName)
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> {
          ConnectorStateInfo info;
          try {
             info = connect.connectorStatus(connectorName);
          } catch (Exception e) {
            log.warn("Failed to get connector status", e);
            return false;
          }
          return info != null
              && info.tasks().size() >= 1
              && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
              && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
        });
  }

  protected void awaitCommittedOffset(
      String connectorName, String topic, int partition, long expectedOffset) {
    await(String.format("Committed topic %s-%d offset", topic, partition))
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> {
          Map<TopicPartition, OffsetAndMetadata> offsets;
          try {
            offsets = kafkaAdmin.listConsumerGroupOffsets(
                    "connect-" + connectorName)
                .partitionsToOffsetAndMetadata()
                .get(50, TimeUnit.MILLISECONDS);
          } catch (Exception e) {
            log.warn("Failed to get offsets", e);
            return false;
          }
          long committedOffset = offsets.entrySet().stream()
              .filter(e -> e.getKey().topic().equals(topic) && e.getKey().partition() == partition)
              .mapToLong(e -> e.getValue().offset())
              .max()
              .orElse(-1);
          if (committedOffset < expectedOffset) {
            log.info("Awaiting while committed offset will be at least {} but now got {}", expectedOffset, committedOffset);
            return false;
          }
          return true;
        });
  }

  private Map<String, String> defaultWorkerProps() {
    Map<String, String> props = new HashMap<>();
    props.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "50");
    return props;
  }
}

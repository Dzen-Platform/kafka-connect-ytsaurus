package ru.dzen.kafka.connect.ytsaurus.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig;
import tech.ytsaurus.client.TableReader;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.ExistsNode;
import tech.ytsaurus.client.request.Format;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.rows.YTreeMapNodeSerializer;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * @author pbk-vitaliy
 */
@Testcontainers
public abstract class BaseYtsaurusConnectorIntegrationTest extends BaseConnectorIntegrationTest {
  @Container
  protected static final YtsaurusContainer<?> ytsaurus = new YtsaurusContainer<>();

  private volatile YTsaurusClient ytsaurusClient;

  @Override
  protected Map<String, String> baseSinkConnectorProps() {
    Map<String, String> props = super.baseSinkConnectorProps();
    props.put(BaseTableWriterConfig.YT_USER, ytsaurus.getUser());
    props.put(BaseTableWriterConfig.YT_TOKEN, ytsaurus.getToken());
    props.put(BaseTableWriterConfig.YT_CLUSTER, ytsaurus.getCluster());
    return props;
  }

  protected CypressAssertions assertCypress() {
    return new CypressAssertions(getClient());
  }

  protected TableAssertions assertTable() {
    return new TableAssertions(getClient());
  }

  protected synchronized YTsaurusClient getClient() {
    if (ytsaurusClient == null) {
      ytsaurusClient = YTsaurusClient.builder()
          .setCluster(ytsaurus.getCluster())
          .setAuth(YTsaurusClientAuth.builder()
              .setUser(ytsaurus.getUser())
              .setToken(ytsaurus.getToken())
              .build())
          .build();
    }
    return ytsaurusClient;
  }

  public static class CypressAssertions {
    private final YTsaurusClient ytsaurusClient;

    public CypressAssertions(YTsaurusClient ytsaurusClient) {
      this.ytsaurusClient = ytsaurusClient;
    }

    public CypressNodeAssertions node(String nodePath) {
      return new CypressNodeAssertions(ytsaurusClient, nodePath);
    }

    public static class CypressNodeAssertions {
      private final YTsaurusClient ytsaurusClient;
      private final YPath nodePath;
      public CypressNodeAssertions(YTsaurusClient ytsaurusClient, String nodePath) {
        this.ytsaurusClient = ytsaurusClient;
        this.nodePath = YPath.simple(nodePath);
      }

      public void exists() {
        ExistsNode existNodeReq = ExistsNode.builder()
            .setPath(nodePath)
            .build();
        Assertions.assertTrue(
            ytsaurusClient.existsNode(existNodeReq).join(),
            String.format("Expected node %s exists, but has not", nodePath)
        );
      }

      public void valueEquals(YTreeNode expected) {
        GetNode getNodeReq = GetNode.builder()
            .setPath(nodePath)
            .build();
        YTreeNode actual = ytsaurusClient.getNode(getNodeReq).join();
        Assertions.assertEquals(
            expected,
            actual,
            String.format("Expected value of the node %s equal to %s but got %s",
                nodePath, expected, actual)
        );
      }
    }
  }

  public static class TableAssertions {
    private final YTsaurusClient ytsaurusClient;
    public TableAssertions(YTsaurusClient ytsaurusClient) {
      this.ytsaurusClient = ytsaurusClient;
    }

    public StaticTableAssertions staticTableWithPath(String path) {
      return new StaticTableAssertions(ytsaurusClient, YPath.simple(path));
    }

    public StaticTableAssertions anyStaticTableInDir(String dir) {
      GetNode getNodeReq = GetNode.builder()
          .setPath(YPath.simple(dir))
          .build();
      YTreeNode dirContent = ytsaurusClient.getNode(getNodeReq).join();
      Assertions.assertTrue(dirContent.isMapNode(), "Expected map node");
      Assertions.assertTrue(dirContent.asMap().size() > 0, "Expected not empty dir");

      return new StaticTableAssertions(
          ytsaurusClient, YPath.simple(dir).child(dirContent.asMap().keySet().iterator().next()));
    }

    public static class StaticTableAssertions {
      private final YTsaurusClient ytsaurusClient;
      private final YPath tablePath;
      public StaticTableAssertions(YTsaurusClient ytsaurusClient, YPath tablePath) {
        this.ytsaurusClient = ytsaurusClient;
        this.tablePath = tablePath;
      }

      public void exists() {
        ReadTable<YTreeMapNode> readTableReq = ReadTable.<YTreeMapNode>builder()
            .setPath(tablePath)
            .setSerializationContext(
                new SerializationContext<>(new YTreeMapNodeSerializer(), Format.ysonBinary()))
            .build();
        TableReader<YTreeMapNode> tableReader = ytsaurusClient.readTable(readTableReq).join();
        try {
          Assertions.assertTrue(tableReader.canRead(), String.format("Expected readable table %s", tablePath));
        } finally {
          tableReader.close().join();
        }
      }

      public ListAssert<YTreeMapNode> rows() {
        ReadTable<YTreeMapNode> readTableReq = ReadTable.<YTreeMapNode>builder()
            .setPath(tablePath)
            .setSerializationContext(
                new SerializationContext<>(new YTreeMapNodeSerializer()))
            .build();
        TableReader<YTreeMapNode> tableReader = ytsaurusClient.readTable(readTableReq).join();
        List<YTreeMapNode> rows = new ArrayList<>();
        try {
          while (tableReader.canRead()) {
            tableReader.readyEvent().join();
            List<YTreeMapNode> currentRows;
            while ((currentRows = tableReader.read()) != null) {
              rows.addAll(currentRows);
            }
          }
          return new ListAssert<>(rows);
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          tableReader.close().join();
        }
      }
    }
  }
}

package ru.dzen.kafka.connect.ytsaurus.staticTables;


import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import ru.dzen.kafka.connect.ytsaurus.common.BaseOffsetsManager;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.LockNode;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.request.LockMode;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

public class DocumentOffsetsManager extends BaseOffsetsManager {

  private final YPath pathToOffsetsDirectory;

  DocumentOffsetsManager(YPath pathToOffsetsDirectory) {
    this.pathToOffsetsDirectory = pathToOffsetsDirectory;
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> getPrevOffsets(ApiServiceTransaction trx,
      Set<TopicPartition> topicPartitions) throws InterruptedException, ExecutionException {
    var res = new HashMap<TopicPartition, OffsetAndMetadata>();
    for (var topicPartition : topicPartitions) {
      var path = String.format("%s/%d", pathToOffsetsDirectory, topicPartition.partition());
      if (trx.existsNode(path).get()) {
        var mapNode = (YTreeMapNode) trx.getNode(path).get();
        var offset = mapNode.get("offset").get().longValue();
        res.put(topicPartition, new OffsetAndMetadata(offset));
      }
    }
    return res;
  }

  @Override
  public void writeOffsets(ApiServiceTransaction trx,
      Map<TopicPartition, OffsetAndMetadata> offsets)
      throws InterruptedException, ExecutionException {
    for (var entry : offsets.entrySet()) {
      var topicPartition = entry.getKey();
      var offsetAndMetadata = entry.getValue();
      var path = String.format("%s/%d", pathToOffsetsDirectory, topicPartition.partition());
      trx.createNode(
          CreateNode.builder().setPath(YPath.simple(path)).setType(CypressNodeType.DOCUMENT)
              .setIgnoreExisting(true).build()).get();
      trx.setNode(path,
          YTree.mapBuilder().key("offset").value(YTree.longNode(offsetAndMetadata.offset()))
              .buildMap()).get();
    }
  }

  @Override
  public void lockPartitions(ApiServiceTransaction trx, Set<TopicPartition> topicPartitions)
      throws InterruptedException, ExecutionException {
    for (var topicPartition : topicPartitions) {
      trx.lockNode(LockNode.builder().setPath(pathToOffsetsDirectory).setMode(LockMode.Shared)
          .setChildKey(String.format("%s-%d", topicPartition.topic(), topicPartition.partition()))
          .build()).get();
    }
  }
}

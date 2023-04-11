package ru.dzen.kafka.connect.ytsaurus.staticTables;


import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.TopicPartition;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeNode;

public class SchemaManager {

  private final YPath pathToSchemasDirectory;

  SchemaManager(YPath pathToSchemasDirectory) {
    this.pathToSchemasDirectory = pathToSchemasDirectory;
  }

  public TableSchema getPrevSchema(ApiServiceTransaction trx)
      throws InterruptedException, ExecutionException {
    var schemas = new ArrayList<YTreeNode>();
    for (var child : trx.listNode(pathToSchemasDirectory).get().asList()) {
      var path = pathToSchemasDirectory.child(child.stringValue());
      if (trx.existsNode(path.toString()).get()) {
        schemas.add(trx.getNode(path).get());
      }
    }
    return new InferredSchemaBuilder(schemas).build();
  }

  public void writeSchema(ApiServiceTransaction trx,
      TableSchema schema, Set<TopicPartition> topicPartitions)
      throws InterruptedException, ExecutionException {
    for (var topicPartition : topicPartitions) {
      var path = pathToSchemasDirectory.child(topicPartition.partition());
      trx.createNode(
          CreateNode.builder().setPath(path).setType(CypressNodeType.DOCUMENT)
              .setIgnoreExisting(true).build()).get();
      trx.setNode(path.toString(), schema.toYTree()).get();
    }
  }
}
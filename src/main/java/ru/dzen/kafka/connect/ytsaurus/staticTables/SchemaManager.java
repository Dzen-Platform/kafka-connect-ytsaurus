package ru.dzen.kafka.connect.ytsaurus.staticTables;

import java.util.concurrent.ExecutionException;
import ru.dzen.kafka.connect.ytsaurus.schema.SchemaUtils;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;

public class SchemaManager {

  private final YPath pathToSchemasDirectory;

  SchemaManager(YPath pathToSchemasDirectory) {
    this.pathToSchemasDirectory = pathToSchemasDirectory;
  }

  public TableSchema getPrevSchema(ApiServiceTransaction trx)
      throws InterruptedException, ExecutionException {
    TableSchema schema = TableSchema.builder().build();
    for (var child : trx.listNode(pathToSchemasDirectory).get().asList()) {
      var path = pathToSchemasDirectory.child(child.stringValue());
      if (trx.existsNode(path.toString()).get()) {
        schema = SchemaUtils.mergeSchemas(schema, TableSchema.fromYTree(trx.getNode(path).get()));
      }
    }
    return schema;
  }

  public void writeSchema(ApiServiceTransaction trx, TableSchema schema)
      throws InterruptedException, ExecutionException {
    var path = pathToSchemasDirectory.child(0);
    trx.createNode(
        CreateNode.builder().setPath(path).setType(CypressNodeType.DOCUMENT)
            .setIgnoreExisting(true).build()).get();
    trx.setNode(path.toString(), schema.toYTree()).get();
  }
}
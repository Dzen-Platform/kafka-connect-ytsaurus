package ru.dzen.kafka.connect.ytsaurus.staticTables.schemaInference;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import ru.dzen.kafka.connect.ytsaurus.common.TableRow;
import ru.dzen.kafka.connect.ytsaurus.schema.SchemaUtils;
import ru.dzen.kafka.connect.ytsaurus.staticTables.SchemaManager;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;

/**
 * @author pbk-vitaliy
 */
public class InferFromFinalizedTable implements SchemaInferenceStrategyImpl {

  private final SchemaManager schemaManager;
  private TableSchema currentSchema;

  public InferFromFinalizedTable(SchemaManager schemaManager) {
    this.schemaManager = schemaManager;
  }

  @Override
  public void init(ApiServiceTransaction tx, YPath tablePath) {
    try {
      currentSchema = schemaManager.getPrevSchema(tx);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void update(ApiServiceTransaction tx, Collection<TableRow> tableRows) {
    for (TableRow r : tableRows) {
      currentSchema = SchemaUtils.mergeSchemas(currentSchema, r.getSchema());
    }
    try {
      schemaManager.writeSchema(tx, currentSchema);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<TableSchema> getSchema(ApiServiceTransaction tx) {
    return Optional.empty();
  }
}

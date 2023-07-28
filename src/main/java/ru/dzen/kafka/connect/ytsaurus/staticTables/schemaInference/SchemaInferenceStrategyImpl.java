package ru.dzen.kafka.connect.ytsaurus.staticTables.schemaInference;

import java.util.Collection;
import java.util.Optional;
import ru.dzen.kafka.connect.ytsaurus.common.TableRow;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;

/**
 * @author pbk-vitaliy
 */
public interface SchemaInferenceStrategyImpl {
  void init(ApiServiceTransaction tx, YPath tablePath);
  void update(ApiServiceTransaction tx, Collection<TableRow> tableRows);
  Optional<TableSchema> getSchema(ApiServiceTransaction tx);
}

package ru.dzen.kafka.connect.ytsaurus.dynamicTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.ExtractField;
import tech.ytsaurus.core.cypress.YPath;

/**
 * @author pbk-vitaliy
 */
public class RecordRouter {

  private final DynTableWriterConfig config;
  private final ExtractField.Value<SinkRecord> tableExtractor;

  public RecordRouter(DynTableWriterConfig config) {
    this.config = config;
    this.tableExtractor = new ExtractField.Value<>();
    final Map<String, String> tableExtractorConfig = new HashMap<>();
    tableExtractorConfig.put("field", config.getString(DynTableWriterConfig.TABLE_ROUTER_FIELD));
    tableExtractor.configure(tableExtractorConfig);
  }

  public List<Destination> route(Collection<SinkRecord> records) {
    Map<YPath, Collection<SinkRecord>> destinations = new HashMap<>();
    for (SinkRecord r : records) {
      YPath tablePath = Optional.ofNullable(tableExtractor.apply(r))
          .map(SinkRecord::value)
          .map(Object::toString)
          .map(t -> config.getOutputDirectory().child(t))
          .orElse(config.getDataQueueTablePath());
      destinations.computeIfAbsent(tablePath, k -> new ArrayList<>()).add(r);
    }

    return destinations.entrySet().stream()
        .map(e -> new Destination(e.getKey(), e.getValue()))
        .collect(Collectors.toList());
  }

  public class Destination {
    private final YPath destinationPath;
    private final Collection<SinkRecord> records;

    public Destination(YPath destinationPath, Collection<SinkRecord> records) {
      this.destinationPath = destinationPath;
      this.records = records;
    }

    public YPath getDestinationPath() {
      return destinationPath;
    }

    public Collection<SinkRecord> getRecords() {
      return records;
    }
  }
}

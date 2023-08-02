package ru.dzen.kafka.connect.ytsaurus.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.transforms.util.Requirements;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig;
import ru.dzen.kafka.connect.ytsaurus.common.BaseTableWriterConfig.OutputFormat;
import ru.dzen.kafka.connect.ytsaurus.common.Util;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * @author pbk-vitaliy
 */
public class SerializationUtils {
  private static final String purpose = "multicolumn serialization";
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final JsonConverter jsonConverter;

  static {
    jsonConverter = new JsonConverter();
    jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
  }

  public static TypedNode serializeToSingleColumn(
      Schema schema, Object value, OutputFormat outputFormat) throws Exception {
    if (value == null) {
      return new TypedNode(TiType.optional(TiType.nullType()), YTree.nullNode());
    }
    if (value instanceof String) {
      return new TypedNode(TiType.optional(TiType.string()), YTree.stringNode((String) value));
    }
    byte[] valueBytes = jsonConverter.fromConnectData(null, schema, value);
    var valueString = new String(valueBytes, StandardCharsets.UTF_8);
    if (outputFormat == BaseTableWriterConfig.OutputFormat.STRING) {
      return new TypedNode(TiType.optional(TiType.string()), YTree.stringNode(valueString));
    }

    JsonNode valueJson = objectMapper.readTree(valueString);
    YTreeNode valueYTreeNode = Util.convertJsonNodeToYTree(valueJson);
    return new TypedNode(Util.getTypeOfNode(valueYTreeNode), valueYTreeNode);
  }

  public static Map<String, TypedNode> serializeToMultipleColumns(
      Schema schema, Object value, OutputFormat outputFormat) throws Exception {
    if (value == null) {
      return Map.of();
    }
    if (value instanceof String) {
      return serializeJsonString((String) value);
    }
    Map<String, TypedNode> columns = new HashMap<>();
    if (schema == null) {
      Map<String, Object> keyParts = Requirements.requireMap(value, purpose);
      for (Map.Entry<String, Object> entry : keyParts.entrySet()) {
        TypedNode columnNode = serializeToSingleColumn(
            null, entry.getValue(), outputFormat);
        columns.put(entry.getKey(), columnNode);
      }
    } else {
      Struct keyStruct = Requirements.requireStruct(value, purpose);
      for (Field field : schema.fields()) {
        TypedNode columnNode = serializeToSingleColumn(
            field.schema(), keyStruct.get(field), outputFormat);
        columns.put(field.name(), columnNode);
      }
    }
    return columns;
  }

  public static Map<String, TypedNode> serializeJsonString(String json) throws Exception {
    JsonNode jsonNode = objectMapper.readTree(json);
    if (!jsonNode.isObject()) {
      throw new DataException("Only json object nodes supported for map serialization, got: "
          + jsonNode.getNodeType());
    }
    Map<String, TypedNode> result = new HashMap<>();
    Iterator<Entry<String, JsonNode>> it = jsonNode.fields();
    while (it.hasNext()) {
      Entry<String, JsonNode> field = it.next();
      YTreeNode valueYTreeNode = Util.convertJsonNodeToYTree(field.getValue());
      result.put(field.getKey(), new TypedNode(Util.getTypeOfNode(valueYTreeNode), valueYTreeNode));
    }
    return result;
  }

  public static TypedNode serializeRecordHeaders(Headers headers) {
    var headersBuilder = YTree.builder().beginList();
    for (Header header : headers) {
      headersBuilder.value(
          YTree.builder().beginList().value(header.key()).value(header.value().toString())
              .buildList());
    }
    return new TypedNode(TiType.optional(TiType.yson()), headersBuilder.buildList());
  }
}

package ru.dzen.kafka.connect.ytsaurus;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * @author pbk-vitaliy
 */
public class RowPredicates {

  public static Predicate<YTreeMapNode> hasColumnContaining(String columnName, String textToSearch) {
    return row -> row.containsKey(columnName)
        && row.get(columnName)
        .filter(YTreeNode::isStringNode)
        .map(v -> v.stringNode().stringValue().contains(textToSearch))
        .orElse(false);
  }

  public static Predicate<YTreeMapNode> hasDifferentIntValues(String columnName) {
    Set<Integer> encounteredValues = new HashSet<>();
    return row -> encounteredValues.add(row.getInt(columnName));
  }
}

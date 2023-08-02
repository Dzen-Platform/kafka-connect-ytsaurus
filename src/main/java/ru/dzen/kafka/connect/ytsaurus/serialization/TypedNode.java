package ru.dzen.kafka.connect.ytsaurus.serialization;

import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * @author pbk-vitaliy
 */
public class TypedNode {
  private final TiType type;
  private final YTreeNode value;

  public TypedNode(TiType type, YTreeNode value) {
    this.type = type;
    this.value = value;
  }

  public TiType getType() {
    return type;
  }

  public YTreeNode getValue() {
    return value;
  }
}

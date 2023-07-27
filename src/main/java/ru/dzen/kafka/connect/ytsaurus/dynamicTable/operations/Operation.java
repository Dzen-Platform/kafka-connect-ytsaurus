package ru.dzen.kafka.connect.ytsaurus.dynamicTable.operations;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author pbk-vitaliy
 */
public enum Operation {
  CREATE("c"),
  READ("r"),
  UPDATE("u"),
  DELETE("d"),
  ;
  private static final Map<String, Operation> opsByLiterals =
      Arrays.stream(Operation.values())
          .collect(Collectors.toMap(o -> o.literal, Function.identity()));
  private final String literal;

  Operation(String literal) {
    this.literal = literal;
  }

  public static Optional<Operation> byLiteral(String literal) {
    return Optional.ofNullable(opsByLiterals.get(literal));
  }
}

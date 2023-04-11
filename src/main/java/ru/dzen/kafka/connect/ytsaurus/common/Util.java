package ru.dzen.kafka.connect.ytsaurus.common;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.LockNode;
import tech.ytsaurus.client.request.LockNodeResult;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

public class Util {

  public static Duration parseHumanReadableDuration(String durationString) {
    durationString = durationString.trim().replaceAll(" ", "").toUpperCase();
    var pattern = Pattern.compile("\\d*[A-Z]");
    var matcher = pattern.matcher(durationString);
    var isDatePart = true;
    var formattedDuration = "P";
    while (matcher.find()) {
      var part = durationString.substring(matcher.start(), matcher.end());
      if (part.charAt(part.length() - 1) != 'D' && isDatePart) {
        isDatePart = false;
        formattedDuration += "T";
      }
      formattedDuration += part;
    }
    return Duration.parse(formattedDuration);
  }

  public static String toHumanReadableDuration(Duration duration) {
    return duration.toString()
        .substring(1)
        .replace("T", "")
        .replaceAll("(\\d[HMS])(?!$)", "$1 ")
        .toLowerCase();
  }

  public static String formatDateTime(Instant timestamp, boolean dateOnly) {
    var localDateTime = LocalDateTime.ofInstant(timestamp, ZoneOffset.UTC);
    var formatter = dateOnly ? DateTimeFormatter.ofPattern("yyyy-MM-dd") :
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    return localDateTime.format(formatter);
  }

  public static Instant floorByDuration(Instant timestamp, Duration duration) {
    var flooredSeconds =
        (timestamp.getEpochSecond() / duration.getSeconds()) * duration.getSeconds();
    return Instant.ofEpochSecond(flooredSeconds);
  }

  public static YTreeNode convertJsonNodeToYTree(JsonNode jsonNode) {
    if (jsonNode.isObject()) {
      var mapBuilder = YTree.mapBuilder();
      jsonNode.fields().forEachRemaining(entry -> {
        var key = entry.getKey();
        var valueNode = entry.getValue();
        mapBuilder.key(key).value(convertJsonNodeToYTree(valueNode));
      });
      return mapBuilder.buildMap();
    } else if (jsonNode.isArray()) {
      var listBuilder = YTree.listBuilder();
      jsonNode.forEach(element -> listBuilder.value(convertJsonNodeToYTree(element)));
      return listBuilder.buildList();
    } else if (jsonNode.isTextual()) {
      return YTree.stringNode(jsonNode.asText());
    } else if (jsonNode.isNumber()) {
      if (jsonNode.isIntegralNumber()) {
        return YTree.longNode(jsonNode.asLong());
      } else {
        return YTree.doubleNode(jsonNode.asDouble());
      }
    } else if (jsonNode.isBoolean()) {
      return YTree.booleanNode(jsonNode.asBoolean());
    } else if (jsonNode.isNull()) {
      return YTree.nullNode();
    } else {
      throw new UnsupportedOperationException(
          "Unsupported JsonNode type: " + jsonNode.getNodeType());
    }
  }

  public static LockNodeResult waitAndLock(ApiServiceTransaction trx, LockNode lockNodeReq,
      Duration timeout) throws Exception {
    var res = trx.lockNode(lockNodeReq).get();
    var deadline = Instant.now().plusMillis(timeout.toMillis());
    while ((timeout.toNanos() == 0) || Instant.now().isBefore(deadline)) {
      var lockStateNodePath = String.format("#%s/@state", res.lockId);
      var state = trx.getNode(lockStateNodePath).get().stringValue();
      if (state.equals("acquired")) {
        break;
      }
      Thread.sleep(2000);
    }
    return res;
  }

  public static <T> T retryWithBackoff(Callable<T> task, int maxRetries, long initialBackoffMillis,
      long maxBackoffMillis) throws Exception {
    Exception lastException = null;
    long backoffMillis = initialBackoffMillis;

    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return task.call();
      } catch (Exception e) {
        lastException = e;
        if (attempt < maxRetries) {
          try {
            TimeUnit.MILLISECONDS.sleep(backoffMillis);
          } catch (InterruptedException ie) {
            // Restore the interrupt status and break the loop
            Thread.currentThread().interrupt();
            break;
          }
          backoffMillis = Math.min(backoffMillis * 2, maxBackoffMillis);
        }
      }
    }

    if (lastException != null) {
      throw lastException;
    } else {
      throw new Exception("Retry operation failed due to interruption.");
    }
  }

  public static void retryWithBackoff(Runnable task, int maxRetries, long initialBackoffMillis,
      long maxBackoffMillis) throws Exception {
    Callable<Void> callableTask = () -> {
      task.run();
      return null;
    };
    retryWithBackoff(callableTask, maxRetries, initialBackoffMillis, maxBackoffMillis);
  }
}

package ru.dzen.kafka.connect.ytsaurus.dynamicTable;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.ApiServiceClient;
import tech.ytsaurus.client.request.FreezeTable;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.UnfreezeTable;
import tech.ytsaurus.client.request.UnmountTable;
import tech.ytsaurus.core.cypress.YPath;

/**
 * @author pbk-vitaliy
 */
public class DyntableUtils {
  private static final Logger log = LoggerFactory.getLogger(DyntableUtils.class);

  public static void freezeTable(ApiServiceClient client, YPath tablePath) {
    log.trace("Freezing table {}", tablePath);
    client.freezeTable(new FreezeTable(tablePath)).join();
    awaitTabletState(client, tablePath, "frozen");
    log.trace("Frozen table {}", tablePath);
  }

  public static void unfreezeTable(ApiServiceClient client, YPath tablePath) {
    log.trace("Unfreezing table {}", tablePath);
    client.unfreezeTable(new UnfreezeTable(tablePath)).join();
    awaitTabletState(client, tablePath, "mounted");
    log.trace("Unfrozen table {}", tablePath);
  }

  public static void unmountTable(ApiServiceClient client, YPath tablePath) {
    log.trace("Unmounting table {}", tablePath);
    client.unmountTable(new UnmountTable(tablePath)).join();
    awaitTabletState(client, tablePath, "unmounted");
    log.trace("Unmounted table {}", tablePath);
  }

  public static void mountTable(ApiServiceClient client, YPath tablePath) {
    log.info("Trying to mount table {}", tablePath);
    client.mountTable(new MountTable(tablePath)).join();
    awaitTabletState(client, tablePath, "mounted");
    log.info("Mounted table {}", tablePath);
  }

  private static void awaitTabletState(ApiServiceClient client, YPath tablePath, String targetState) {
    Duration maxWait = Duration.ofMinutes(1);
    Duration checkPeriod = Duration.ofMillis(500);
    long deadline = System.currentTimeMillis() + maxWait.toMillis();
    while (System.currentTimeMillis() < deadline) {
      String currentState = client.getNode(tablePath.attribute("tablet_state")).join().stringValue();
      if (targetState.equals(currentState)) {
        break;
      }
      try {
        Thread.sleep(checkPeriod.toMillis());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

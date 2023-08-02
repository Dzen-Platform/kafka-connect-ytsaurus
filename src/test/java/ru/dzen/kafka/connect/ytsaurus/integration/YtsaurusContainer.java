package ru.dzen.kafka.connect.ytsaurus.integration;

import java.io.IOException;
import java.net.ServerSocket;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * @author pbk-vitaliy
 */
public class YtsaurusContainer<SELF extends YtsaurusContainer<SELF>> extends GenericContainer<SELF> {

  private static final String dockerHostname = "localhost";
  private static final String ytsaurusFqdn = dockerHostname;

  private static final String proxyConfigTemplate =
      "{address_resolver={enable_ipv4=%%true;enable_ipv6=%%false;};coordinator={public_fqdn=\"%s:%d\"}}";
  private final int proxyPort;

  private final int rpcProxyPort;
  private final String localCypressDir;
  public YtsaurusContainer() {
    this(DockerImageName.parse("ytsaurus/local:stable"));
  }

  public YtsaurusContainer(DockerImageName dockerImageName) {
    this(dockerImageName, "", 1);
  }

  public YtsaurusContainer(
      DockerImageName dockerImageName,
      String localCypressDir,
      int rpcProxyCount)
  {
    super(dockerImageName);
    this.localCypressDir = localCypressDir;
    this.proxyPort = getRandomOpenPort();
    this.rpcProxyPort = getRandomOpenPort();
    addFixedExposedPort(proxyPort, 80);
    addFixedExposedPort(rpcProxyPort, rpcProxyPort);
    self()
        .withFileSystemBind(localCypressDir, "/var/lib/yt/local-cypress")
        .withCommand(
            "--fqdn " + ytsaurusFqdn +
                " --proxy-config " + String.format(proxyConfigTemplate, dockerHostname, proxyPort) +
                " --rpc-proxy-count " + rpcProxyCount +
                " --rpc-proxy-port " + rpcProxyPort)
        .waitingFor(Wait.forLogMessage(".*Environment started.*", 1));
  }

  public String getYtsaurusFqdn() {
    return ytsaurusFqdn;
  }

  public int getProxyPort() {
    return proxyPort;
  }

  public int getRpcProxyPort() {
    return rpcProxyPort;
  }

  public String getLocalCypressDir() {
    return localCypressDir;
  }

  public String getCluster() {
    return String.format("http://%s:%d", getYtsaurusFqdn(), getProxyPort());
  }

  public String getUser() {
    return "root";
  }

  public String getToken() {
    return "";
  }

  private int getRandomOpenPort() {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      return serverSocket.getLocalPort();
    } catch (IOException e) {
      throw new RuntimeException("Failed to open random port");
    }
  }
}

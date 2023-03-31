# Quick Start Guide

## Prerequisites

### YTsaurus Prerequisites

Docker 19 or higher.

### Confluent Platform Prerequisites

Kafka Connect.

## Create a Dockerized YTsaurus Cluster

To create a Dockerized YTsaurus cluster, follow these steps:

1. Download `run_local_cluster.sh`:

   ```
   wget https://raw.githubusercontent.com/ytsaurus/ytsaurus/main/yt/docker/local/run_local_cluster.sh
   ```

2. Run the script:

   ```
   ./run_local_cluster.sh
   ```

## Install and Load the connector

1. [Build](README.md#build-from-source) the plugin or download the [prebuilt jar](README.md#download-the-prebuilt-jar).

2. Follow the manual connector installation [instructions](https://docs.confluent.io/home/connect/install.html).

3. Create a `yt-sink.json` file with the following contents:

    ```json
    {
      "name": "yt-sink",
      "config": {
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "connector.class": "ru.dzen.kafka.connect.ytsaurus.YtTableSinkConnector",
        "tasks.max": "1",
        "topics": "<topics>",
        "yt.connection.user": "<user>",
        "yt.connection.token": "<token>",
        "yt.connection.cluster": "127.0.0.1",
        "yt.sink.output.type": "STATIC_TABLES",
        "yt.sink.output.directory": "//tmp/example_delivery",
        "yt.sink.output.table.schema.type": "WEAK",
        "yt.sink.output.key.format": "ANY",
        "yt.sink.output.value.format": "ANY",
        "yt.sink.output.ttl": "6h",
        "yt.sink.static.rotation.period": "5m"
      }
    }
    ```

4. Load the YTsaurus Sink Connector.

    ```bash
    curl -X POST -H "Content-Type: application/json" --data-binary "@yt-sink.json" http://localhost:8083/connectors
    ```

5. Check the status of the connector to confirm that it is in a RUNNING state.

    ```bash
    curl http://localhost:8083/connectors/yt-sink/status
    ```

    Your output should resemble the following:

    ```json
    {
      "name": "yt-sink",
      "connector": {
        "state": "RUNNING",
        "worker_id": "127.0.0.1:8083"
      },
      "tasks": [
        {
          "id": 0,
          "state": "RUNNING",
          "worker_id": "127.0.0.1:8083"
        }
      ],
      "type": "sink"
    }
    ```

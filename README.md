# Kafka Connect YTsaurus Sink Connector

Kafka Connect YTsaurus Sink Connector is a [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) plugin for writing data from Kafka topics to static or ordered dynamic tables in [YTsaurus](https://github.com/ytsaurus/ytsaurus).


## Features

- **Guaranteed exactly-once delivery**

    Apache Kafka topic offsets are saved in YTsaurus, ensuring *at-most-once* delivery. Kafka Connect provides *at-least-once* delivery, and together, they achieve *exactly-once* delivery guarantee.

- **Support for static and dynamic tables**

    Data can be written to [static](https://ytsaurus.tech/docs/en/user-guide/storage/static-tables) or [ordered dynamic](https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/ordered-dynamic-tables) tables, as defined by the `yt.sink.output.type` [configuration property](#configuration-properties).

- **Configurable output formats and schema types**

    Data can be written in various formats, defined via the `yt.sink.output.table.schema.type` [configuration property](#configuration-properties):

    - 'unstructured': Messages from Apache Kafka are written in separate columns in YTsaurus tables, along with metadata columns.

    - 'weak': Messages from Apache Kafka are written across multiple columns in YTsaurus tables, based on the inferred record schema from JSON-like input messages. Currently incompatible with dynamic tables.

- **Automatic output table creation**

    Output tables are automatically generated in YTsaurus.

- **Time-to-Live (TTL) for output tables or rows**

    Data is automatically deleted from YTsaurus after a time period defined by the `yt.sink.output.ttl` [configuration property](#configuration-properties).

## Prerequisites

The Kafka Connect cluster must utilize Java 11 or higher.

## Install YTsaurus Sink Connector

### Install the Connector Manually

1. Copy the JAR file to the Kafka Connect classpath (usually located in `/usr/share/java/kafka`):

   ```
   cp <path_to_your_jar_file> /usr/share/java/kafka/
   ```

2. Create a connector using the Kafka Connect REST interface ([API documentation](https://docs.confluent.io/platform/current/connect/references/restapi.html)) and include the following configuration:

   ```
   "connector.class": "ru.dzen.kafka.connect.ytsaurus.YtTableSinkConnector"
   ```

3. Modify other configuration options as required for your specific use case. For example, set the appropriate topic, output type, and other options in the connector configuration file.

For more information and examples, refer to the Kafka Connect [official documentation](https://docs.confluent.io/kafka-connectors/self-managed/install.html#install-self-managed-connectors).

### Install the Connector Using Confluent Hub

Coming soon

## Obtain a JAR File

### Download the Prebuilt JAR

<!-- - From Maven:

  Download the `ru.dzen.kafka.connect.ytsaurus` JAR file from Maven using the command line with `wget` or `curl`. Replace `<version>` with the desired version number and `<filename>` with the appropriate JAR filename:

  - wget:
  
    ```
      wget https://repo1.maven.org/maven2/ru/dzen/kafkaConnectYtsaurus/<version>/<filename>.jar
    ```

  - curl:

    ```
      curl -O https://repo1.maven.org/maven2/ru/dzen/kafkaConnectYtsaurus/<version>/<filename>.jar
    ``` -->

- From GitHub Release:

  Download the JAR file from a GitHub release using the command line with `wget` or `curl`:

  - wget:

    ```
      wget https://github.com/Dzen-Platform/kafka-connect-ytsaurus/releases/download/1.1.0/kafka-connect-ytsaurus-1.1.0.jar
    ```

  - curl:

    ```
      curl -L -O https://github.com/Dzen-Platform/kafka-connect-ytsaurus/releases/download/1.1.0/kafka-connect-ytsaurus-1.1.0.jar
    ```

### Build from Source

1. Clone the repository.

   ```bash
   git clone https://github.com/dzen-platform/kafka-connect-ytsaurus
   cd kafka-connect-yt
   ```

2. Compile the JAR file. The following methods are available:

  - using Gradle;

    Prerequisites: 

    - JDK 11 or higher.
    - Make.

    Execute the following command:

    ```bash
    make USE_GRADLE=true
    ```
  - using Docker;

    Prerequisites:

    - Docker 19 or higher,
    - Make.

    Execute the following command:

    ```bash
    make USE_GRADLE=false
    ```
  Upon successful completion, you will find the JAR file at the path `build/libs/kafka-connect-ytsaurus.jar`.

## Quick Start

For an overview, explore the 5-minute [Kafka Connect YTsaurus Full Example](examples/full/readme.md).

For a more comprehensive introduction, follow the [Quick Start Guide](quickstart.md).

## Configuration Properties

### Common Configuration Properties

| Property | Description | Type | Default | Importance | Required |
| -- | -- | -- | -- | -- | -- |
| `yt.connection.user` | Username for the YT API authentication | string | - | HIGH | yes |
| `yt.connection.token` | Access token for the YT API authentication | password | - | HIGH | yes | 
| `yt.connection.cluster` | Identifier of the YT cluster to connect to | string | - | HIGH | yes | 
| `yt.sink.output.type` | Specifies the output type ('dynamic_table' or 'static_tables') | string | 'dynamic_table' | HIGH | no | 
| `yt.sink.output.key.format` | Determines the output format for keys ('string' or 'any') | string | 'any' | HIGH | no | 
| `yt.sink.output.value.format` | Determines the output format for values ('string' or 'any') | string | 'any' | HIGH | no | 
| `yt.sink.output.table.schema.type` | Defines the schema type for output tables ('unstructured' or 'weak'; 'strict' value to be supported later) | string | 'unstructured' | HIGH | no | 
| `yt.sink.output.directory` | Specifies the output directory path | string | - | HIGH | yes | 
| `yt.sink.metadata.directory.name` |  The name of the metadata subdirectory in the output directory | string | '__ connect_s ink_metadata __' | MEDIUM | no | 
| `yt.sink.output.ttl` | Time-to-live (TTL) for output tables or rows, specified as a duration (e.g., '30d' for 30 days) | string | '30d' | MEDIUM | no | 

### Configuration Properties for Dynamic Tables

| Property | Description | Type | Default | Importance | Required |
| -- | -- | -- | -- | -- | -- |
| `yt.sink.dynamic.queue.postfix` | The data queue table name in the output directory | string | 'queue' | MEDIUM | no |
| `yt.sink.dynamic.queue.auto.create` | Flag to automatically create and mount dynamic tables if they do not exist | boolean | true | MEDIUM | no | 
| `yt.sink.dynamic.queue.tablet.count` | The number of tablets for the data queue table in dynamic output mode | int | 1 | MEDIUM | no | 

### Configuration Properties for Static Tables

| Property | Description | Type | Default | Importance | Required |
| -- | -- | -- | -- | -- | -- |
| `yt.sink.static.rotation.period` | Rotation period | string | - | HIGH | yes |
| `yt.sink.static.tables.dir.postfix` | The name of the static tables subdirectory in the output directory | string | 'output' | MEDIUM | no |
| `yt.sink.static.compression.codec` | Compression codec of the output tables | string | zstd | MEDIUM | no |
| `yt.sink.static.tables.optimize.for` | Specifies the storage optimization strategy for the table. Choose 'lookup' for row-based table storage optimized for point lookups, or 'scan' for column-based table storage optimized for scans and aggregations. | string | lookup | MEDIUM | no |
| `yt.sink.static.tables.replication.factor` | The replication factor of the output tables | int | - | MEDIUM | no |
| `yt.sink.static.tables.erasure.codec` | Erasure coding codec of the output tables | string | - | MEDIUM | no |
| `yt.sink.static.merge.chunks` | Activate the consolidation of chunks during the table rotation process | boolean | false | MEDIUM | no |
| `yt.sink.static.schema.inference.strategy` | The strategy for inferring the schema of the output tables. Valid options are DISABLED, INFER_FROM_FIRST_BATCH, and INFER_FROM_FINALIZED_TABLE. Schema inference strategy could be used only with STRICT output table schema type. <br><br>DISABLED means that the schema will not be inferred at all, and the output tables will have a weak schema that only includes the column names. <br><br>INFER_FROM_FIRST_BATCH means that the table schema will be created from the first batch of data, and will not change after the table is created. <br><br>INFER_FROM_FINALIZED_TABLE means that the weak schema will be used during the writing of rows, and after rotation, the finalized table will be re-merged with the schema based on all rows of the table. If using INFER_FROM_FINALIZED_TABLE, chunks will be merged. | string | DISABLED | HIGH | no |


<!-- > **Warning**
>
> The connector has `chunk_merger_mode` set to 'auto' for YTsaurus output folders in order to enhance writing efficiency. To make the optimization work, it is necessary to activate the *chunk_merger* functionality on YTsaurus side. -->

## Todo

- Publish the plugin on Confluent Hub.
- Implement support for [sorted dynamic tables](https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/sorted-dynamic-tables).
- Implement a source connector.
- Implement integration tests.
- Add more unit tests.

## Troubleshooting

If you encounter issues while using this repository, follow the steps below to troubleshoot:

1. Check the README and documentation.

    Ensure you have followed the instructions provided in the current README, the [Kafka Connect YTsaurus Full Example](examples/full/readme.md), the [Quick Start Guide](quickstart.md), or other relevant documentation.

2. Search for existing issues.

    Before opening a new issue, search the existing issues in the repository to see if someone has already reported the same problem.

3. Open a new issue.

    If you cannot find an existing issue that matches your problem, open a new issue in the repository. Provide as much information as possible, including:

    - a clear and concise description of the issue;
    - steps to reproduce the problem;
    - any error messages or logs;
    - your environment (OS, software versions, etc.);
    - possible solutions or workarounds you have already tried.

<!-- TODO:
4. **Ask for help in the community**: If you still cannot resolve the issue, consider asking for help in the project's community channels:
- **telegram**:  -->

4. Contribute.

    If you identify a bug or have a feature request, consider contributing to the repository by opening a pull request with your proposed changes.

When seeking help or reporting issues, always be respectful, patient, and provide as much information as possible to help others understand and resolve the problem.

## License

YTsaurus Sink Connector is an open-source connector that is distributed under the Apache 2.0 license. For more information, see the project [license](LICENSE).

# Kafka Connect YTsaurus Full Example

This example demonstrates the plugin usage, featuring Kafka, Zookeeper, Kafka Connect, Kafka REST Proxy, YTsaurus, and a simple data producer.

## Prerequisites

Ensure that Docker (version 19.03.0 or higher) and Docker Compose are installed on your system.

## Setup and Running

Follow these steps to set up and run the example:

1. Start all services, including Kafka, Zookeeper, Kafka Connect, Kafka REST Proxy, YTsaurus, and the data producer:

   ```
   make up
   ```

2. Create connectors:

   There are several example connector configurations provided in configs directory:

   1. `yt-sink-static-json-schema.json`: for static tables with an unstructured schema
      ```
      make create-connector CONNECTOR_NAME=yt-sink-static-json-schema
      ```
      Next, generate messages using the command provided below:
      ```
      make produce-json-schema-messages SCHEMA_NAME=test-json-schema
      ```
   2. `yt-sink-static-inferred.json`: for static tables with a strict schema inferred from the first batch processed
      ```
      make create-connector CONNECTOR_NAME=yt-sink-static-inferred
      ```
   3. `yt-sink-static-inferred-from-finalized-table.json`: for static tables with a strict schema inferred from all rows processed
      ```
      make create-connector CONNECTOR_NAME=yt-sink-static-inferred-from-finalized-table
      ```
   4. `yt-sink-static-unstructured.json`: for static tables with an unstructured schema
      ```
      make create-connector CONNECTOR_NAME=yt-sink-static-unstructured
      ```
   5. `yt-sink-static-weak.json`: for static tables with a weak schema
      ```
      make create-connector CONNECTOR_NAME=yt-sink-static-weak
      ```
   6. `yt-sink-dynamic-unstructured.json`: for dynamic tables with an unstructured schema
      ```
      make create-connector CONNECTOR_NAME=yt-sink-dynamic-unstructured
      ```
   You can concurrently configure multiple connectors by using the command provided below. This command allows you to configure all connectors simultaneously:
   ```
   make create-all-connectors
   make produce-messages-for-all-schemas
   ```

3. Display the Kafka Connect service logs:

   ```
   make connect-logs
   ```

   After executing this command, you should see the following output:

   ```
   connect  | [2023-03-27 16:03:59,111] INFO Done processing batch in 0.575s: 190 total, 190 written, 0 skipped (common.ru.dzen.kafka.connect.ytsaurus.BaseTableWriter)
   ...
   ```

4. Check the outputs:
   1. Using UI

      Just open <http://localhost:8001/ui/navigation?path=//home>

   2. Using CLI (only for **static** tables)

      To print the first ten lines of the first output table created by the sink connector run the following command:

      ```
      make print-output-table CONNECTOR_NAME=<connector_name>
      ```
      Replace <connector_name> with the appropriate name, which should match the configuration file name. For example, use `yt-sink-static-weak` if the configuration file is named `yt-sink-static-weak.json`.

      After running this command, you should see newline-delimited JSON data similar to the following:

      ```
      {"_headers":[],"_partition":0,"_topic":"test","data":{"age":"28 years old","friends":["Dannie, Pennie","Amara, Aurlie","Brit, Jillian","Blinni, Yetty","Deeanne, Florrie"],"home":{"country":"Belize","address":"314 example street"}},"_offset":0,"num":1,"i":0,"_key":"\"msg0\"","_timestamp":1679933023845}
      ...
      ```

5. Stop all services:

   ```
   make down
   ```

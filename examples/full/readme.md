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

2. Create the `yt-sink` connector to write data from the `test` topic into static tables. The connector configuration can be found in the `yt-sink.json` file:

   ```
   make create-connector
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

4. Show the first ten lines of the first output table created by the sink connector:

   ```
   make print-output-table
   ```

   After running this command, you should see newline-delimited JSON data similar to the following:

   ```
   {"_headers":[],"_partition":0,"_topic":"test","data":{"age":"28 years old","friends":["Dannie, Pennie","Amara, Aurlie","Brit, Jillian","Blinni, Yetty","Deeanne, Florrie"],"home":{"country":"Belize","address":"314 example street"}},"_offset":0,"num":1,"i":0,"_key":"\"msg0\"","_timestamp":1679933023845}
   ...
   ```

5. Stop all services:

   ```
   make down
   ```

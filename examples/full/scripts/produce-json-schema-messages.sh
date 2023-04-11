#!/bin/bash
set -e

schema_name=$1
schema=$(cat /schema.json)

cat /data.ndjson | kafka-json-schema-console-producer --bootstrap-server broker:29092 \
	--property schema.registry.url=http://localhost:8081 --topic "$schema_name" \
	--property value.schema="$schema"

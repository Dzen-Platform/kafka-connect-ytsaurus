#!/bin/bash

# Wait for Kafka Connect to start by checking HTTP endpoint
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://127.0.0.1:8083)" != "200" ]]; do
  echo "Kafka Connect is not yet available, waiting 5 seconds..."
  sleep 5
done

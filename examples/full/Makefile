# Define phony targets to prevent conflicts with file names
.PHONY: up down create-connector connect-logs wait build build-images print-output-table produce-json-schema-messages

# Bring up the docker compose environment
up: build
	docker compose up -d

# Bring down the docker compose environment and remove orphans
down:
	docker compose down --remove-orphans

# Create specific YTsaurus sink connector
create-connector: wait
	@if [ -z "$(CONNECTOR_NAME)" ]; then \
		echo "Error: Please provide a connector name using CONNECTOR_NAME=<connector_name>"; \
		exit 1; \
	fi; \
	curl --fail -X POST -H "Content-Type: application/json" --data-binary "@configs/$(CONNECTOR_NAME).json" http://localhost:8083/connectors

create-all-connectors: wait
	@for file in configs/*; do \
	filename=$$(basename "$$file"); \
	filename="$${filename%.*}"; \
	$(MAKE) create-connector CONNECTOR_NAME="$$filename" || true; \
	done

# Remove the YTsaurus sink connector
remove-connector: wait
	@if [ -z "$(CONNECTOR_NAME)" ]; then \
		echo "Error: Please provide a connector name using CONNECTOR_NAME=<connector_name>"; \
		exit 1; \
	fi; \
	curl --fail -v -X DELETE http://localhost:8083/connectors/$(CONNECTOR_NAME)

remove-all-connectors:
	@for file in configs/*; do \
	filename=$$(basename "$$file"); \
	filename="$${filename%.*}"; \
	$(MAKE) remove-connector CONNECTOR_NAME="$$filename" || true; \
	done

# Follow the logs of the Kafka Connect service
connect-logs:
	docker compose logs --follow connect

# Wait for the Kafka Connect service to start
wait:
	@./scripts/wait-for-kafka-connect.sh

# Build the docker images for the environment
build: build-images
	docker compose build

# Build the docker images for each component of the environment
build-images:
	cd images/kafka-connect-with-yt-plugin && $(MAKE)
	cd images/fixed-yt-local && $(MAKE)
	cd images/producer && $(MAKE)
	cd images/yt-client && $(MAKE)

# Print the first ten lines of the first output table created by the sink connector
print-output-table:
	@if [ -z "$(CONNECTOR_NAME)" ]; then \
		echo "Error: Please provide a connector name using CONNECTOR_NAME=<connector_name>"; \
		exit 1; \
	fi; \
	TABLE_NAME=$$(docker compose exec -it yt-client yt list //home/$(CONNECTOR_NAME)/test/output | head -n 1); \
	docker compose exec -it yt-client yt read-table --format json "//home/$(CONNECTOR_NAME)/test/output/$$TABLE_NAME" | head -n 10;

produce-json-schema-messages:
	@if [ -z "$(SCHEMA_NAME)" ]; then \
		echo "Error: Please provide a schema name using SCHEMA_NAME=<schema_name>"; \
		exit 1; \
	fi; \
	docker compose cp "schemas/$$SCHEMA_NAME.json" schema-registry:/schema.json; \
	docker compose cp "data/$$SCHEMA_NAME.ndjson" schema-registry:/data.ndjson; \
	docker compose cp "scripts/produce-json-schema-messages.sh" schema-registry:/; \
	docker compose exec -it schema-registry /produce-json-schema-messages.sh $$SCHEMA_NAME

produce-messages-for-all-schemas: wait
	@for file in schemas/*; do \
	schema_name=$$(basename "$$file"); \
	schema_name="$${schema_name%.*}"; \
	$(MAKE) produce-json-schema-messages SCHEMA_NAME="$$schema_name" || true; \
	done
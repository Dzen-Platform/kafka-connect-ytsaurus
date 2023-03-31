# Define phony targets to prevent conflicts with file names
.PHONY: up down create-connector connect-logs wait build build-images print-output-table

# Bring up the docker-compose environment
up: build
	docker-compose up -d

# Bring down the docker-compose environment and remove orphans
down:
	docker-compose down --remove-orphans

# Create the YTsaurus sink connector
create-connector: wait
	curl --fail -v -X POST -H "Content-Type: application/json" --data-binary "@yt-sink.json" http://localhost:8083/connectors

# Remove the YTsaurus sink connector
remove-connector: wait
	curl --fail -v -X DELETE http://localhost:8083/connectors/yt-sink

# Follow the logs of the Kafka Connect service
connect-logs:
	docker-compose logs --follow connect

# Wait for the Kafka Connect service to start
wait:
	./scripts/wait-for-kafka-connect.sh

# Build the docker images for the environment
build: build-images
	docker-compose build

# Build the docker images for each component of the environment
build-images:
	cd kafka-connect-with-yt-plugin && $(MAKE)
	cd fixed-yt-local && $(MAKE)
	cd producer && $(MAKE)
	cd yt-client && $(MAKE)

# Print the first ten lines of the first output table created by the sink connector
print-output-table:
	@TABLE_NAME=$$(docker-compose exec -it yt-client yt list //home/yt-sink/test/output | head -n 1); \
	docker-compose exec -it yt-client yt read-table --format json "//home/yt-sink/test/output/$$TABLE_NAME" | head -n 10;

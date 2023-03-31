# Set the name of the Docker image used to build the connector
IMAGE_NAME=kafka-connect-ytsaurus-builder

# Define the build target
.PHONY: build build-image clean

# Target to build the Docker image
build-image:
	# Build the Docker image using the specified name
	docker build -t $(IMAGE_NAME) .

# Target to build the connector JAR file
build: build/libs/kafka-connect-ytsaurus.jar

# Recipe to build the connector JAR file
build/libs/kafka-connect-ytsaurus.jar: build-image
	# Create a temporary directory to hold the exported Docker image
	mkdir -p ./tmp
	# Create a Docker container using the builder image
	CONTAINER_ID=$$(docker create $(IMAGE_NAME)); \
	# Export the contents of the container to a tar file
	docker export $$CONTAINER_ID -o ./tmp/image.tar; \
	# Remove the container
	docker rm $$CONTAINER_ID;
	# Extract the connector JAR file from the exported tar file
	tar -xf ./tmp/image.tar -C ./tmp app/build/libs/*
	# Create the build/libs directory if it does not exist
	mkdir -p build/libs
	# Move the connector JAR file to the build/libs directory
	mv ./tmp/app/build/libs/* build/libs/
	# Remove the temporary directory
	rm -rf tmp

# Target to clean up build artifacts
clean:
	# Remove the temporary directory
	-rm -rf tmp
	# Remove the connector JAR file
	-rm build/libs/kafka-connect-ytsaurus.jar
	# Remove the Docker image used to build the connector
	-docker rmi $(IMAGE_NAME)

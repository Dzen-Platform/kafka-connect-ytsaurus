# Set the name of the Docker image used to build the connector
IMAGE_NAME=kafka-connect-ytsaurus-builder

# Check if JDK is installed and its version is greater than or equal to 11
JDK_VERSION_MAJOR=$(shell javac -version 2>&1 | awk '{print $$2}' | awk -F '.' '{print $$1}')

ifeq ($(origin USE_GRADLE), undefined)
  ifeq ($(shell [ -z "$(JDK_VERSION_MAJOR)" ] || [ "$(JDK_VERSION_MAJOR)" -lt "11" ] && echo false || echo true),true)
    USE_GRADLE=true
  else
    USE_GRADLE=false
  endif
endif

# Define the build target
.PHONY: build build-image clean

# Target to build the connector JAR file
build: build/libs/kafka-connect-ytsaurus.jar

# Target to build the Docker image
build-image:
	# Build the Docker image using the specified name
	docker build -t $(IMAGE_NAME) .

# Recipe to build the connector JAR file
build/libs/kafka-connect-ytsaurus.jar:
ifeq ($(USE_GRADLE), true)
	./gradlew shadowJar
else
	$(MAKE) build-image
	# Create a temporary directory to hold the exported Docker image
	mkdir -p ./tmp
	CONTAINER_ID=$$(docker create $(IMAGE_NAME)); \
	docker export $$CONTAINER_ID -o ./tmp/image.tar; \
	docker rm $$CONTAINER_ID;
	# Extract the connector JAR file from the exported tar file
	tar -xf ./tmp/image.tar -C ./tmp app/build/libs/*
	# Create the build/libs directory if it does not exist
	mkdir -p build/libs
	# Move the connector JAR file to the build/libs directory
	mv ./tmp/app/build/libs/* build/libs/
	# Remove the temporary directory
	rm -rf tmp
endif

# Target to clean up build artifacts
clean:
	# Remove the temporary directory
	-rm -rf tmp
	# Remove the connector JAR file
	-rm build/libs/kafka-connect-ytsaurus.jar
	# Remove the Docker image used to build the connector
	-docker rmi $(IMAGE_NAME)

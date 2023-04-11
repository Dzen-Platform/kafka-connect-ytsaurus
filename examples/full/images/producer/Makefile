# Define variables
DOCKER_IMAGE_NAME := yt-sink-example-producer

# Define targets
.PHONY: all build clean

# Define the default target
all: build

# Define the build target
build:
	docker build -t $(DOCKER_IMAGE_NAME) .

# Define the clean target
clean:
	docker rmi $(DOCKER_IMAGE_NAME)

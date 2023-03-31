# Define variables
IMAGE_NAME = ytsaurus-client

# Define targets
.PHONY: all build clean

# Define the default target
all: build

# Define the build target
build:
	docker build -t $(IMAGE_NAME) .

# Define the clean target
clean:
	docker rmi $(IMAGE_NAME)

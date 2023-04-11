export DOCKER_BUILDKIT=0

IMAGE_NAME=kafka-connect-with-yt-plugin

.PHONY: build clean

build: copy-jar
	docker build -t $(IMAGE_NAME) .

copy-jar: build-jar
	mkdir -p ./temp_data
	cp ../../../../build/libs/kafka-connect-ytsaurus.jar ./temp_data/

build-jar:
	cd ../../../../ && $(MAKE)

clean:
	docker rmi $(IMAGE_NAME)
	rm -rf ./temp_data

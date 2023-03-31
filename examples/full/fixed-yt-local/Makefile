IMAGE_NAME=fixed-yt-local

.PHONY: build
build:
	docker build -t $(IMAGE_NAME) .

.PHONY: clean
clean:
	docker rmi $(IMAGE_NAME)

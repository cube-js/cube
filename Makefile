CUBE_VERSION=$(shell node -e "console.log(require('./packages/cubejs-docker/package.json').version);")
GIT_REV := $(shell git rev-parse --short HEAD)
DIRTY_FLAG := $(shell git diff HEAD --quiet || echo '-dirty')
IMAGE_VERSION=${CUBE_VERSION}-${GIT_REV}${DIRTY_FLAG}

IMAGE=889818756387.dkr.ecr.us-east-1.amazonaws.com/incognia/cube:${IMAGE_VERSION}
CUBESTORE_IMAGE=889818756387.dkr.ecr.us-east-1.amazonaws.com/incognia/cubestore:${IMAGE_VERSION}

.PHONY: build push cubestore/build cubestore/push

build: cubestore/build
	docker build -t ${IMAGE} . -f incognia.Dockerfile --build-arg IMAGE_VERSION=${IMAGE_VERSION}

cubestore/build:
	docker build -t ${CUBESTORE_IMAGE} rust/cubestore/

cubestore/push:
	docker push ${CUBESTORE_IMAGE}

push: build cubestore/push
	docker push ${IMAGE}

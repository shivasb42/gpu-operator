# Image URL to use all building/pushing image targets
IMG ?= gpu-operator:latest

.PHONY: all
all: build

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test: fmt vet
	go test ./... -coverprofile cover.out

.PHONY: build
build: fmt vet
	go build -o bin/manager ./cmd

.PHONY: run
run: fmt vet
	go run ./cmd

.PHONY: docker-build
docker-build:
	docker build -t ${IMG} .

.PHONY: docker-push
docker-push:
	docker push ${IMG}

.PHONY: install
install:
	kubectl apply -f config/crd/bases/gpu.operator.io_gpujobs.yaml

.PHONY: uninstall
uninstall:
	kubectl delete -f config/crd/bases/gpu.operator.io_gpujobs.yaml --ignore-not-found=true

.PHONY: deploy
deploy: install
	kubectl apply -k config/default

.PHONY: undeploy
undeploy:
	kubectl delete -k config/default --ignore-not-found=true

.PHONY: samples
samples:
	kubectl apply -f config/samples/gpujob_trainingjob.yaml

.PHONY: tidy
tidy:
	go mod tidy

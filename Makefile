BUILD_DIR := $(PWD)/build
ifdef REBUILD
	BUILD_FLAGS = -a
endif

.PHONY: all
all: test build

.PHONY: clean
clean:
	rm -rf build/

.PHONY: build-local
build-local: build

.PHONY: build
build: $(PWD)/db/main.go | $(clean)
	mkdir -p $(BUILD_DIR)
	go build $(BUILD_FLAGS) -o $(BUILD_DIR)/dbserver db/main.go

.PHONY: build-static
build-static: $(PWD)/db/main.go | $(clean)
	mkdir -p $(PWD)/build
	CGO_ENABLED=0 GOOS=linux GOARCH=386 go build $(BUILD_FLAGS) -installsuffix cgo -ldflags '-s' \
	 -o $(BUILD_DIR)/dbserver_static db/main.go

.PHONY: build-docker
build-docker: $(PWD)/docker/db/Dockerfile
	docker build -t rrowniak/gopicosql:latest . -f $(PWD)/docker/db/Dockerfile

.PHONY: build-integration-tests-docker
build-integration-tests-docker: $(PWD)/docker/db/Dockerfile
	docker build -t rrowniak/gopicosql-int-tests:latest . -f $(PWD)/docker/tests/Dockerfile

.PHONY: integration-tests-docker-compose
integration-tests-docker-compose: build-integration-tests-docker
	docker-compose -f ./docker/tests/integration-tests.yaml up --build --remove-orphans  --abort-on-container-exit

TEST_DIRS = $(shell go list -f 'TEST-{{.ImportPath}}' ./...)
.PHONY: $(TEST_DIRS)
$(TEST_DIRS): | $(clean)
	$(eval import_path := $(subst TEST-,,$@))
	$(eval test_name := $(subst /,_,$@))
	@echo '---> Running $(test_name) test...'
	@cd ../$(import_path); go test -coverprofile=$(BUILD_DIR)/coverage-$(test_name).out .
#	go test -o $(BUILD_DIR)/$(import_path).test -c $(import_path)

.PHONY: test
test: $(TEST_DIRS)

.PHONY: test-cov-html
test-cov-html: $(TEST_DIRS)
#	@for f in $(shell ls ${BUILD_DIR}/coverage-TEST-*.out); do go tool cover -html=$${f}; done
	$(PWD)/tools/gocovmerge.sh
	go tool cover -html=${BUILD_DIR}/coverage-TEST-MERGED.out

.PHONY: run
run:
	cd $(PWD)/db && go run main.go
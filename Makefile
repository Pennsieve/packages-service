.PHONY: help clean test test-ci package publish

LAMBDA_BUCKET ?= "pennsieve-cc-lambda-functions-use1"
WORKING_DIR   ?= "$(shell pwd)"
API_DIR ?= "api"

# Service Lambda
SERVICE_NAME  ?= "packages-service"
SERVICE_EXEC  ?= "packages_service"
SERVICE_PACK  ?= "packagesService"
SERVICE_PACKAGE_NAME  ?= "${SERVICE_NAME}-${IMAGE_TAG}.zip"

# Restore Package Lambda
RESTORE_NAME  ?= "restore-package"
RESTORE_EXEC  ?= "restore_package"
RESTORE_PACK  ?= "restorePackage"
RESTORE_PACKAGE_NAME  ?= "${RESTORE_NAME}-${IMAGE_TAG}.zip"

.DEFAULT: help

help:
	@echo "Make Help for $(SERVICE_NAME)"
	@echo ""
	@echo "make clean			- spin down containers and remove db files"
	@echo "make test			- run dockerized tests locally"
	@echo "make test-ci			- run dockerized tests for Jenkins"
	@echo "make package			- create venv and package lambda function"
	@echo "make publish			- package and publish lambda function"

# Start the local versions of docker services
local-services:
	docker compose -f docker-compose.test.yml down --remove-orphans
	docker compose -f docker-compose.test.yml up -d pennsievedb minio dynamodb

# Run tests locally
test: local-services
	./run-tests.sh localtest.env .env
	docker compose -f docker-compose.test.yml down --remove-orphans
	make clean

# Run test coverage locally
test-coverage: local-services
	./run-test-coverage.sh localtest.env
	docker compose -f docker-compose.test.yml down --remove-orphans
	make clean

# Run dockerized tests (used on Jenkins)
test-ci:
	docker compose -f docker-compose.test.yml down --remove-orphans
	@IMAGE_TAG=$(IMAGE_TAG) docker-compose -f docker-compose.test.yml up --exit-code-from=ci-tests ci-tests

clean: docker-clean
	rm -fR lambda/bin

# Spin down active docker containers.
docker-clean:
	docker compose -f docker-compose.test.yml down

# Build lambda and create ZIP file
package:
	@echo ""
	@echo "*******************************"
	@echo "*   Building service lambda   *"
	@echo "*******************************"
	@echo ""
	cd ${WORKING_DIR}/lambda/service; \
  		env GOOS=linux GOARCH=amd64 go build -o $(WORKING_DIR)/lambda/bin/$(SERVICE_PACK)/$(SERVICE_EXEC); \
		cd $(WORKING_DIR)/lambda/bin/$(SERVICE_PACK)/ ; \
			zip -r $(WORKING_DIR)/lambda/bin/$(SERVICE_PACK)/$(SERVICE_PACKAGE_NAME) .
	@echo ""
	@echo "***************************************"
	@echo "*   Building restore package lambda   *"
	@echo "***************************************"
	@echo ""
	cd ${WORKING_DIR}/lambda/restore; \
  		env GOOS=linux GOARCH=amd64 go build -o $(WORKING_DIR)/lambda/bin/$(RESTORE_PACK)/$(RESTORE_EXEC); \
		cd $(WORKING_DIR)/lambda/bin/$(RESTORE_PACK)/ ; \
			zip -r $(WORKING_DIR)/lambda/bin/$(RESTORE_PACK)/$(RESTORE_PACKAGE_NAME) .

# Copy Service lambda to S3 location
publish: package
	@echo ""
	@echo "******************************************"
	@echo "*   Publishing packages-service lambda   *"
	@echo "******************************************"
	@echo ""
	aws s3 cp $(WORKING_DIR)/lambda/bin/$(SERVICE_PACK)/$(SERVICE_PACKAGE_NAME) s3://$(LAMBDA_BUCKET)/$(SERVICE_NAME)/
	rm -rf $(WORKING_DIR)/lambda/bin/$(SERVICE_PACK)/$(SERVICE_PACKAGE_NAME)
	@echo ""
	@echo "*****************************************"
	@echo "*   Publishing restore package lambda   *"
	@echo "*****************************************"
	@echo ""
	aws s3 cp $(WORKING_DIR)/lambda/bin/$(RESTORE_PACK)/$(RESTORE_PACKAGE_NAME) s3://$(LAMBDA_BUCKET)/$(SERVICE_NAME)/
	rm -rf $(WORKING_DIR)/lambda/bin/$(RESTORE_PACK)/$(RESTORE_PACKAGE_NAME)

# Run go mod tidy on modules
tidy:
	cd ${WORKING_DIR}/lambda/service; go mod tidy
	cd ${WORKING_DIR}/lambda/restore; go mod tidy
	cd ${WORKING_DIR}/api; go mod tidy


#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = Totetsys_ETL
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip

# Additional variables for Docker and ECR
AWS_ACCOUNT_ID := $(shell aws sts get-caller-identity --query "Account" --output text --profile $(PROFILE))
ECR_REPO_NAME := transform_lambda_func
IMAGE_TAG := latest
IMAGE_URI_TRANSFORM := $(AWS_ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/transform_lambda_func:$(IMAGE_TAG)
IMAGE_URI_LOAD := $(AWS_ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/load_lambda_func:$(IMAGE_TAG)





# Create python interpreter environment.
# 	Checks Python version.
# 	Sets up a virtual environment using virtualenv.
# 	Installs virtualenv and virtualenvwrapper via pip (in silent mode -q).
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate


# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-enviroment requirements-extract requirements-transform requirements-load

requirements-extract: create-environment
	$(call execute_in_env, $(PIP) install -r lambda/extract/requirements.txt)
requirements-transform: create-environment
	$(call execute_in_env, $(PIP) install -r lambda/transform/requirements.txt)
requirements-load: create-environment
	$(call execute_in_env, $(PIP) install -r lambda/load/requirements.txt)

## remove the environment requirements
uninstall-requirements:uninstall-requirements-extract uninstall-requirements-transform uninstall-requirements-load

uninstall-requirements-extract: create-environment
	$(call execute_in_env, $(PIP) uninstall -r lambda/extract/requirements.txt -y)
uninstall-requirements-transform: create-environment
	$(call execute_in_env, $(PIP) uninstall -r lambda/transform/requirements.txt -y)
uninstall-requirements-load: create-environment
	$(call execute_in_env, $(PIP) uninstall -r lambda/load/requirements.txt -y)

################################################################################################################
# Set Up
## Install bandit, a security linter for Python code.
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install safety, a tool to check dependencies for known vulnerabilities.
safety:
	$(call execute_in_env, $(PIP) install safety)

## Install black, a code formatter.
black:
	$(call execute_in_env, $(PIP) install black)


## Install flake8, a pep8 check.
flake8:
	$(call execute_in_env, $(PIP) install flake8)


## Install pytest
pytest:
	$(call execute_in_env, $(PIP) install pytest)


## Set up dev requirements (bandit, safety, black)
dev-setup: bandit safety black flake8 pytest

# Build / Run

## Run the security test (bandit + safety)
security-test: security-test-extract security-test-transform security-test-load
security-test-extract:
	$(call execute_in_env, safety check -r lambda/extract/requirements.txt --ignore=70612)
	$(call execute_in_env, bandit -lll lambda/extract/*.py)
security-test-transform:
	$(call execute_in_env, safety check -r lambda/transform/requirements.txt --ignore=70612)
	$(call execute_in_env, bandit -lll lambda/transform/*.py)
security-test-load:
	$(call execute_in_env, safety check -r lambda/load/requirements.txt --ignore=70612)
	$(call execute_in_env, bandit -lll lambda/load/*.py)
	

## Pep8 tests
pep8-test: pep8-test-extract pep8-test-transform pep8-test-load
pep8-test-extract:
	$(call execute_in_env, flake8 lambda/extract/**/*.py)
pep8-test-transform:
	$(call execute_in_env, flake8 lambda/transform/**/*.py)
pep8-test-load:
	$(call execute_in_env, flake8 lambda/load/**/*.py)

## Run the black code check
run-black: run-black-extract run-black-transform run-black-load
run-black-extract:
	$(call execute_in_env, black --line-length 79 ./lambda/extract/**/*.py)
run-black-transform:
	$(call execute_in_env, black --line-length 79 ./lambda/transform/**/*.py)
run-black-load:
	$(call execute_in_env, black --line-length 79 ./lambda/load/**/*.py)

## Run the unit tests
unit-test-extract:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH}/lambda/extract:${PYTHONPATH}/lambda/extract/src pytest lambda/extract -vvrP --testdox)
unit-test-transform:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH}/lambda/transform:${PYTHONPATH}/lambda/transform/src pytest lambda/transform -vvrP --testdox)
unit-test-load:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH}/lambda/load:${PYTHONPATH}/lambda/load/src pytest lambda/load -vvrP --testdox)

## Run all checks
run-checks: security-test unit-test pep8-test
run-checks-extract:security-test-extract unit-test-extract pep8-test-extract
run-checks-transform:security-test-transform unit-test-transform pep8-test-transform
run-checks-load:security-test-load unit-test-load pep8-test-load

# Build the Docker image
build-transform:
	docker build -f Dockerfile.transform -t transform_lambda_func .
build-load:
	docker build -f Dockerfile.load -t load_lambda_func .

# Tag the Docker image
tag-transform:
	docker tag transform_lambda_func:latest $(IMAGE_URI_TRANSFORM)
tag-load:
	docker tag load_lambda_func:latest $(IMAGE_URI_LOAD)

# Login to Amazon ECR
ecr-login:
	aws ecr get-login-password --region $(REGION) | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com

# Push the Docker image to ECR
push-transform: ecr-login
# sudo usermod -aG docker $USER
#	This will only work if you grant docker SUDO priveleges with the command above
	docker push $(IMAGE_URI_TRANSFORM)

push-load: ecr-login
	docker push $(IMAGE_URI_LOAD)


deploy-transform: build-transform tag-transform push-transform
deploy-load: build-load tag-load ecr-login push-load

debug:
	@echo "AWS_ACCOUNT_ID: $(AWS_ACCOUNT_ID)"
	@echo "REGION: $(REGION)"
	@echo "PROFILE: $(PROFILE)"
	@echo "IMAGE_URI: $(IMAGE_URI)"


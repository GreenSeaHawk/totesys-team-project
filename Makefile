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
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r lambda/extract/requirements.txt)
	$(call execute_in_env, $(PIP) install -r lambda/load/requirements.txt)
	$(call execute_in_env, $(PIP) install -r lambda/transform/requirements.txt)

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
security-test:
	$(call execute_in_env, safety check -r lambda/extract/requirements.txt)
	$(call execute_in_env, safety check -r lambda/load/requirements.txt)
	$(call execute_in_env, safety check -r lambda/transform/requirements.txt)
	$(call execute_in_env, bandit -lll lambda/**/*.py)


## Run the black code check
run-black:
	$(call execute_in_env, black --line-length 79 ./lambda/**/*.py)

## Run the unit tests
unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest lambda/extract -vv)
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest lambda/load -vv)
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest lambda/transform -vv)

## Run all checks
run-checks: security-test unit-test 
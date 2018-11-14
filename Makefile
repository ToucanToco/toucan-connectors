.PHONY         = set-test-env circleci-test test clean docker-clean build upload list docker-test docker-run
.DEFAULT_GOAL := list

sudo = disable # Execute Docker with sudo

########################################

SHELL          = /bin/bash
VENV_NAME      = .venv
CODECOV_TOKEN  = 3e56598d-bfe8-4741-a973-f4b70bd2c280
DOCKER_COMPOSE = docker-compose
DOCKER         = docker

ifeq ($(sudo),enable)
        DOCKER_COMPOSE := sudo -E ${DOCKER_COMPOSE}
        DOCKER := sudo -E ${DOCKER}
endif

########################################

set-test-env:
	python3 -m venv ${VENV_NAME}
	${VENV_NAME}/bin/python3 -m pip install --upgrade pip setuptools
	${VENV_NAME}/bin/python3 setup.py install
	${VENV_NAME}/bin/python3 -m pip install -r requirements-testing.txt
	${VENV_NAME}/bin/python3 -m pip install .[all] --process-dependency-links
	${VENV_NAME}/bin/python3 -m pip install codecov

test:
	${VENV_NAME}/bin/flake8 toucan_connectors tests
	PYTHONPATH=. ${VENV_NAME}/bin/pytest tests

docker-test: docker-run set-test-env test docker-clean

circleci-test: set-test-env test
	source ${VENV_NAME}/bin/activate && \
	codecov --token=${CODECOV_TOKEN}

clean:
	find . -name "*~" -delete -or -name ".*~" -delete
	find . -name '*.pyc' -delete
	find . -name __pycache__ -delete
	rm -rf .pytest_cache
	rm -rf build dist toucan_connectors.egg-info

docker-run:
	cd tests && \
	${DOCKER_COMPOSE} pull && \
	${DOCKER_COMPOSE} up -d

docker-clean:
	-@cd tests && \
	${DOCKER_COMPOSE} down --remove-orphans
	-@${DOCKER} rmi $$(docker images -q --filter "dangling=true")
	-@${DOCKER} rm $$(docker ps -q -f status=exited)
	-@${DOCKER} volume ls -qf dangling=true | xargs -r ${DOCKER} volume rm

build:
	${VENV_NAME}/bin/python setup.py sdist bdist_wheel

upload:
	twine upload dist/*

list:
	@grep '^[^\.#[:space:]].*:' Makefile | \
		cut -d':' -f1

new_connector:  # $ make new_connector type=Magento
 ifeq (${type},)
	@echo -e "Missing type option:\n\tmake $@ type=Magento"
	exit 1
 endif
	${eval MODULE=`echo "${type}" | tr A-Z a-z`}
	mkdir toucan_connectors/${MODULE}
	touch toucan_connectors/${MODULE}/__init__.py
	m4 -DTYPE=${type} templates/connector.py.m4 > toucan_connectors/${MODULE}/${MODULE}_connector.py
	mkdir tests/${MODULE}
	m4 -DTYPE=${type} templates/tests.py.m4 > tests/${MODULE}/test_${MODULE}.py

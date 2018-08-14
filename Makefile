.PHONY         = set-test-env circleci-test test clean docker-clean build upload list
.DEFAULT_GOAL := list

########################################

SHELL         = /bin/bash
VENV_NAME     = .venv
CODECOV_TOKEN = 3e56598d-bfe8-4741-a973-f4b70bd2c280

########################################

set-test-env:
	python3 -m venv ${VENV_NAME}
	${VENV_NAME}/bin/python3 -m pip install --upgrade pip setuptools
	${VENV_NAME}/bin/python3 setup.py install
	${VENV_NAME}/bin/python3 -m pip install -r requirements-testing.txt
	${VENV_NAME}/bin/python3 -m pip install .[all]
	${VENV_NAME}/bin/python3 -m pip install codecov

test: set-test-env
	${VENV_NAME}/bin/flake8 toucan_connectors tests
	PYTHONPATH=. ${VENV_NAME}/bin/pytest tests

circleci-test: test
	${VENV_NAME}/bin/codecov --token=${CODECOV_TOKEN}

clean:
	find . -name "*~" -delete -or -name ".*~" -delete
	find . -name '*.pyc' -delete
	find . -name __pycache__ -delete
	rm -rf .pytest_cache
	rm -rf build dist toucan_connectors.egg-info

docker-clean:
	-@docker rmi $$(docker images -q --filter "dangling=true")
	-@docker rm $$(docker ps -q -f status=exited)
	-@docker volume ls -qf dangling=true | xargs -r docker volume rm

build:
	${VENV_NAME}/bin/python setup.py sdist bdist_wheel

upload:
	twine upload dist/*

list:
	@grep '^[^\.#[:space:]].*:' Makefile | \
		cut -d':' -f1

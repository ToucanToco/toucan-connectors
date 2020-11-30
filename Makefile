.DEFAULT_GOAL := all
isort = isort toucan_connectors tests setup.py
black = black toucan_connectors tests setup.py

.PHONY: clean
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -rf .coverage build dist *.egg-info .pytest_cache .mypy_cache

.PHONY: install
install:
	pip3 install -U setuptools pip==20.2
	pip3 install -r requirements-testing.txt --ignore-installed
	pip3 install '.[all]'

.PHONY: format
format:
	$(isort)
	$(black)

.PHONY: lint
lint:
	flake8 toucan_connectors tests setup.py
	$(isort) --check-only
	$(black) --check

.PHONY: test
test:
	pytest --junitxml=test-report.xml --cov=toucan_connectors --cov-report xml

.PHONY: all
all: test lint

.PHONY: new_connector
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

.PHONY: build
build:
	python setup.py sdist bdist_wheel

.PHONY: upload
upload:
	twine upload dist/*

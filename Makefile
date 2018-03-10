test:
	flake8 toucan_connectors tests
	PYTHONPATH=. pytest tests

clean:
	find . -name "*~" -delete -or -name ".*~" -delete
	find . -name '*.pyc' -delete
	find . -name __pycache__ -delete

docker-clean:
	-@docker rmi $$(docker images -q --filter "dangling=true")
	-@docker rm $$(docker ps -q -f status=exited)
	-@docker volume ls -qf dangling=true | xargs -r docker volume rm

build:
	python setup.py sdist bdist_wheel

upload:
	twine upload dist/*

.DEFAULT_GOAL := build
PIP_CMD="python3 -m pip install -r"
SUB_REQUIREMENTS="find connectors/ -type f -name requirements.txt -execdir $(PIP_CMD) requirements.txt \;"

build:
	eval "$(PIP_CMD) ./main_requirements.txt"
	eval $(SUB_REQUIREMENTS)

test: build
	PYTHONPATH=. pytest tests/mysql --no-pull -s -v

clean:
	find . -name "*~" -delete -or -name ".*~" -delete
	find . -name '*.pyc' -delete
	find . -name __pycache__ -delete
	-@docker rmi $$(docker images -q --filter "dangling=true")
	-@docker rm $$(docker ps -q -f status=exited)
	-@docker volume ls -qf dangling=true | xargs -r docker volume rm

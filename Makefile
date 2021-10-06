pwd := $(shell pwd)

all: dev test

test: venv
	. venv/bin/activate &&  pytest

venv:
	test -d venv || python3 -m venv venv

dev: venv
	. venv/bin/activate && pip install -r requirements.txt -r requirements-dev.txt

clean:
	rm -rf venv
	find . -iname "*.pyc" -delete

itest: clean
	docker run  -v $(pwd):/test/ -it cassandra-table-properties-test

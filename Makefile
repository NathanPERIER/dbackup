
PYTHON = python3

VENV_DIR = ./venv
VENV_PYTHON = ${VENV_DIR}/bin/python3

DOCKER_TAG = dbackup


.PHONY: setup pip docker

setup:
	${PYTHON} -m venv ${VENV_DIR}
	make pip

pip:
	${VENV_PYTHON} -m pip install -U -r ./requirements.txt

docker:
	docker build . -t ${DOCKER_TAG}

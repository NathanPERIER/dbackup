FROM python:alpine

ARG POSTGRES_VERSION=17

ENV DBACKUP_CONFIG_PATH=/etc/dbackup/dbackup.yaml
ENV DBACKUP_OUTPUT_DIR=/output

RUN --mount=type=bind,source=./requirements.txt,dst=/tmp/install/requirements.txt \
    apk update \
 && apk add --no-cache mariadb-client postgresql${POSTGRES_VERSION}-client \
 && pip3 install -r /tmp/install/requirements.txt --break-system-packages \
 && mkdir -p /opt/dbackup

WORKDIR /opt/dbackup

COPY dbackup.py ./

ENTRYPOINT ["python3", "./dbackup.py"]

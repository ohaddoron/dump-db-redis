FROM python:3.9.10-slim

COPY . /opt/project
WORKDIR /opt/project

RUN pip install -r requirements.txt

ENTRYPOINT [ "python", "code/dump_to_redis.py" ]


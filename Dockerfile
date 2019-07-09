FROM alpine:3.10.0

RUN apk add build-base postgresql-dev python3 python3-dev --no-cache

COPY . /app
WORKDIR /app
RUN pip3 install -e .

ENV BULK_SIZE 2000
ENV MAX_WORKERS 4

ENTRYPOINT ["hive2elastic_post"]

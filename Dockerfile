FROM alpine:3.9

RUN \
    apk --no-cache add \
    python3 \
    python3-dev \
    postgresql-dev \
    gcc \
    g++ \
    musl-dev \
    supervisor

ADD . /app

WORKDIR /app

RUN \
    pip3 install . && \
    chmod +x /app/start.sh

CMD ["/usr/bin/supervisord", "-n", "-c", "/app/supervisord.conf"]

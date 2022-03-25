FROM alpine:3.9

RUN \
    apk --no-cache add \
    python3 \
    python3-dev \
    postgresql-dev \
    gcc \
    g++ \
    musl-dev \
    ca-certificates \
    py3-openssl \
    nginx \
    fcgiwrap \
    supervisor

ADD . /app

WORKDIR /app

RUN \
    pip3 install . && \
    mkdir -p /run/nginx/ && \
    chmod +x /app/healthcheck.sh && \
    chmod +x /app/start.sh

CMD ["/usr/bin/supervisord", "-n", "-c", "/app/supervisord.conf"]

FROM alpine:3.22

RUN apk add --no-cache curl jq

COPY init/register-connector.sh /usr/local/bin/register-connector
COPY postgres.config.json /etc/kafka-connect/postgres.config.json

RUN chmod +x /usr/local/bin/register-connector

ENTRYPOINT ["/usr/local/bin/register-connector"]

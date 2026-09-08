# Kafka Connect with Debezium PostgreSQL and the Confluent Avro converter.
# Debezium dropped Confluent JARs from the image in 2.0; the Hub plugin
# ships the converter plus its dependencies as one isolated plugin dir.
ARG DEBEZIUM_VERSION=3.6.1.Final
FROM quay.io/debezium/connect:${DEBEZIUM_VERSION}

ARG CONFLUENT_AVRO_CONVERTER_VERSION=8.3.1
USER root
RUN curl -fsSL \
      "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/${CONFLUENT_AVRO_CONVERTER_VERSION}/confluentinc-kafka-connect-avro-converter-${CONFLUENT_AVRO_CONVERTER_VERSION}.zip" \
      -o /tmp/avro-converter.zip \
    && python3 -m zipfile -e /tmp/avro-converter.zip /kafka/connect \
    && rm /tmp/avro-converter.zip
USER kafka

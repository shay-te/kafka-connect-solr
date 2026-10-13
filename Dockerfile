ARG BUILD_IMAGE=maven:3.9-eclipse-temurin-17
ARG RUNTIME_IMAGE=confluentinc/cp-kafka-connect-base:7.5.1

FROM ${BUILD_IMAGE} AS build
WORKDIR /src
COPY pom.xml ./
COPY src ./src
COPY config ./config
# The plugin is the assembly's lib/: the connector jar plus SolrJ and every other runtime dependency.
RUN mvn -B -q -DskipTests package \
    && mkdir /assembly && cd /assembly && jar xf /src/target/*-package.zip \
    && mv /assembly/*/lib /plugin-lib

FROM ${RUNTIME_IMAGE}
LABEL maintainer="una"
USER root
RUN mkdir -p /usr/share/confluent-hub-components/una-kafka-connect-solr/lib
COPY --from=build /plugin-lib/ /usr/share/confluent-hub-components/una-kafka-connect-solr/lib/
COPY config/quickstart-solr.properties /etc/kafka-connect-solr/quickstart-solr.properties
USER appuser

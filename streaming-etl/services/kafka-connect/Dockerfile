FROM confluentinc/cp-kafka-connect:7.1.1

WORKDIR /usr/share/java

RUN curl -s -O https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/1.9.0.Final/debezium-connector-postgres-1.9.0.Final-plugin.tar.gz
RUN tar xzf debezium-connector-postgres-1.9.0.Final-plugin.tar.gz

USER root

WORKDIR /usr/share/java/kafka

RUN curl -s -O https://packages.confluent.io/maven/io/confluent/kafka-connect-jdbc/10.4.0/kafka-connect-jdbc-10.4.0.jar
RUN curl -s -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.4/postgresql-42.3.4.jar

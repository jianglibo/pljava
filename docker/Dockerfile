# docker build -t builder.resp.me/datafocus/postgresql_11_with_java_11:vx ./
# docker push builder.resp.me/datafocus/postgresql_11_with_java_11:vx

FROM docker.io/bitnami/java:11-prod AS builder

FROM docker.io/bitnami/postgresql:11

COPY --from=builder /opt/bitnami/java /opt/bitnami/java

COPY pljava.policy /opt/bitnami/postgresql/etc/pljava.policy

COPY init.sql /init.sql

COPY json-20210307.jar /opt/jars/json-20210307.jar
COPY pljava-examples-1.6-SNAPSHOT.jar /opt/jars/pljava-examples-1.6-SNAPSHOT.jar
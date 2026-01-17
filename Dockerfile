FROM eclipse-temurin:21-jdk-alpine AS app-builder
RUN apk add --no-cache git
CMD ["git","--version"]

COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY ohsome-contributions/pom.xml ohsome-contributions/pom.xml
COPY ohsome-parquet/pom.xml ohsome-parquet/pom.xml
COPY ohsome-planet-cli/pom.xml ohsome-planet-cli/pom.xml
COPY ohsome-replication/pom.xml ohsome-replication/pom.xml
COPY ohsome-changesets/pom.xml ohsome-changesets/pom.xml
COPY ohsome-replication-update/pom.xml ohsome-replication-update/pom.xml
COPY ohsome-output-minio/pom.xml ohsome-output-minio/pom.xml
COPY ohsome-output/pom.xml ohsome-output/pom.xml
COPY osm-changesets/pom.xml osm-changesets/pom.xml
COPY osm-geometry/pom.xml osm-geometry/pom.xml
COPY osm-pbf/pom.xml osm-pbf/pom.xml
COPY osm-types/pom.xml osm-types/pom.xml
COPY osm-xml/pom.xml osm-xml/pom.xml

RUN ./mvnw dependency:go-offline

COPY .git ./
COPY ohsome-contributions/src ohsome-contributions/src
COPY ohsome-parquet/src ohsome-parquet/src
COPY ohsome-planet-cli/src ohsome-planet-cli/src
COPY ohsome-replication/src ohsome-replication/src
COPY ohsome-changesets/src ohsome-changesets/src
COPY ohsome-replication-update/src ohsome-replication-update/src
COPY ohsome-output-minio/src ohsome-output-minio/src
COPY ohsome-output/src ohsome-output/src
COPY osm-changesets/src osm-changesets/src
COPY osm-geometry/src osm-geometry/src
COPY osm-pbf/src osm-pbf/src
COPY osm-types/src osm-types/src
COPY osm-xml/src osm-xml/src

RUN ./mvnw package -DskipTests


FROM eclipse-temurin:21-alpine AS jre-builder

RUN $JAVA_HOME/bin/jlink \
    --add-modules java.base \
    --add-modules java.logging \
    --add-modules java.management \
    --add-modules java.naming \
    --add-modules java.sql \
    --add-modules java.xml \
    --add-modules jdk.crypto.ec \
    --add-modules jdk.unsupported \
    --strip-debug \
    --no-man-pages \
    --no-header-files \
    --compress=2 \
    --output /javaruntime

FROM alpine:3
RUN --mount=type=cache,target=/etc/apk/cache apk add --update-cache libstdc++

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"
COPY --from=jre-builder /javaruntime $JAVA_HOME

COPY --from=app-builder ohsome-planet-cli/target/ohsome-planet.jar /ohsome-planet.jar

ENTRYPOINT ["java","-jar","/ohsome-planet.jar"]

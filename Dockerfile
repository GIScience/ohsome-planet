FROM eclipse-temurin:21-jdk-alpine AS app-builder

COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY ohsome-contributions/pom.xml ohsome-contributions/pom.xml
COPY ohsome-parquet/pom.xml ohsome-parquet/pom.xml
COPY ohsome-planet-cli/pom.xml ohsome-planet-cli/pom.xml
COPY osm-changesets/pom.xml osm-changesets/pom.xml
COPY osm-geometry/pom.xml osm-geometry/pom.xml
COPY osm-pbf/pom.xml osm-pbf/pom.xml
COPY osm-types/pom.xml osm-types/pom.xml
COPY osm-xml/pom.xml osm-xml/pom.xml

RUN ./mvnw dependency:go-offline

COPY . .

RUN ./mvnw package -DskipTests


FROM eclipse-temurin:21-alpine AS jre-builder

RUN $JAVA_HOME/bin/jlink \
    --add-modules java.base \
    --strip-debug \
    --no-man-pages \
    --no-header-files \
    --compress=2 \
    --output /javaruntime

FROM alpine:3
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"
COPY --from=jre-builder /javaruntime $JAVA_HOME

COPY --from=app-builder ohsome-planet-cli/target/ohsome-planet.jar /ohsome-planet.jar

ENTRYPOINT ["java","-jar","/ohsome-planet.jar"]

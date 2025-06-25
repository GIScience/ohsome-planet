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


FROM eclipse-temurin:21-jre-alpine

RUN --mount=type=cache,target=/etc/apk/cache apk add --update-cache libstdc++

COPY --from=app-builder ohsome-planet-cli/target/ohsome-planet.jar /ohsome-planet.jar

ENTRYPOINT ["java","-jar","/ohsome-planet.jar"]

FROM openjdk:21
COPY . .
RUN ./mvnw clean package -DskipTests

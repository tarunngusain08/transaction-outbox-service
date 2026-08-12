FROM eclipse-temurin:21-jdk@sha256:efd34b940f2d5a621605c8531c2afb7759c936b6c2ef637a69aa3bf3e1e789d1 AS build
WORKDIR /workspace
RUN apt-get update \
    && apt-get install --yes --no-install-recommends unzip \
    && rm -rf /var/lib/apt/lists/*
COPY .mvn .mvn
COPY mvnw pom.xml ./
RUN ./mvnw --batch-mode --no-transfer-progress dependency:go-offline
COPY src src
RUN ./mvnw --batch-mode --no-transfer-progress clean package -DskipUnitTests=true

FROM eclipse-temurin:21-jre@sha256:8cef5fc7bebe421363ab543a2f4db5caf7d119d8db67d56b0f56c485d2de4d55
WORKDIR /app
RUN apt-get update \
    && apt-get install --yes --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 spring
COPY --from=build --chown=spring:spring /workspace/target/transaction-outbox-service-*.jar app.jar
USER spring
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

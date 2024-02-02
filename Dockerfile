# syntax=docker/dockerfile:1

FROM gradle:8.8-jdk21 AS build
WORKDIR /workspace
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
RUN chmod +x gradlew
COPY src ./src
RUN ./gradlew bootJar --no-daemon -x test

FROM eclipse-temurin:21-jre
RUN addgroup --system cauri && adduser --system --ingroup cauri cauri
WORKDIR /app
COPY --from=build /workspace/build/libs/*.jar app.jar
USER cauri

ENV SERVICE_NAME=payments-worker \
    PORT=8080 \
    JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70"

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]

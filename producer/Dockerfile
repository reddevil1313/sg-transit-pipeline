FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY gradlew settings.gradle build.gradle ./
COPY gradle gradle
RUN ./gradlew --no-daemon dependencies > /dev/null
COPY src src
RUN ./gradlew --no-daemon installDist

FROM eclipse-temurin:21-jre
COPY --from=build /src/build/install/producer /app
ENTRYPOINT ["/app/bin/producer"]

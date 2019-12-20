# Install dependencies and build jar

FROM gradle:jdk11 as build

WORKDIR /build
COPY src/ /build/src
COPY build.gradle .
RUN gradle build --no-daemon

# Main Dockerfile

FROM amazoncorretto:11

WORKDIR /app

COPY --from=build /build/build/libs/build.jar /app/kvsrecord.jar

ENTRYPOINT ["java", "-jar", "/app/kvsrecord.jar"]
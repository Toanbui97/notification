FROM docker.int.itech.asia/library/eclipse-temurin:21-jre-jammy

WORKDIR /app
ENV REDIS_HOST=host.docker.internal
ENV REDIS_PORT=6379

COPY ./build/libs/*.jar /app/app.jar

ENTRYPOINT ["java", "-Xms512m", "-Xmx1g", "-XX:+UseG1GC", "-jar", "/app/app.jar"]
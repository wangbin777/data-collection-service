# ---------------------------------------------
# Build stage: compile the Spring Boot fat jar
# ---------------------------------------------
FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /workspace
COPY pom.xml .
COPY src ./src
RUN mvn -B -DskipTests clean package

# ---------------------------------------------
# Runtime stage: lightweight JRE image
# ---------------------------------------------
FROM eclipse-temurin:17-jre-jammy
ENV APP_HOME=/opt/app \
    JAVA_OPTS=""
WORKDIR ${APP_HOME}
RUN groupadd --system spring && useradd --system --gid spring --home ${APP_HOME} spring
COPY --from=builder /workspace/target/*.jar app.jar
RUN chown -R spring:spring ${APP_HOME}
USER spring
EXPOSE 9090
ENTRYPOINT ["sh","-c","java $JAVA_OPTS -jar /opt/app/app.jar"]

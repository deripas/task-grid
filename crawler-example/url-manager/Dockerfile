# url-manager image: copies the build output, run ./mvnw package first (make stand does). Context - this directory.
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY target/url-manager.jar ./
# HTTP (actuator, POST /fetch), gRPC of the node for sidecars, Pekko remoting (cluster mode)
EXPOSE 8080 9090 25520
ENTRYPOINT ["java", "--enable-native-access=ALL-UNNAMED", "-jar", "/app/url-manager.jar"]

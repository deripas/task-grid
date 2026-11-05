# fetcher image: copies the build output, run ./mvnw package first (make stand does). Context - this directory.
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY target/lib ./lib
COPY target/fetcher.jar ./
EXPOSE 50051
ENTRYPOINT ["java", "--enable-native-access=ALL-UNNAMED", "-jar", "/app/fetcher.jar"]

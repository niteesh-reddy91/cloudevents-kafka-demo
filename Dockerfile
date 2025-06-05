FROM openjdk:17-jdk-slim

WORKDIR /app

# Copy your Maven files
COPY pom.xml .
COPY src ./src

# Install Maven
RUN apt-get update && apt-get install -y maven

# Build the application
RUN mvn clean compile

# Run the application
CMD ["mvn", "exec:java", "-Dexec.mainClass=com.example.App"]
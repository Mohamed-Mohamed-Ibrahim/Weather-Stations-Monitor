FROM openjdk:21-jdk-slim

# Set working directory
WORKDIR /app

# Copy the JAR file
COPY consumer.jar /app/consumer.jar

# Run the JAR file
CMD ["java", "-jar", "consumer.jar"]
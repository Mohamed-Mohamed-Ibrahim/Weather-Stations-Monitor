# Dockerfile for BaseCentralStation
FROM openjdk:21

# Set working directory
WORKDIR /app

# Copy the JAR file
COPY central_station.jar /app/central_station.jar

# Create data directory
RUN mkdir -p /data

# Set the data directory as a volume mount point
VOLUME ["/data"]

# Expose any ports if needed (optional)
EXPOSE 8080

# Run the JAR file
CMD ["java", "-jar", "central_station.jar"]
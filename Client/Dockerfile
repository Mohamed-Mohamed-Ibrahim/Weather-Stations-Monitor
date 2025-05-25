# Dockerfile for ELS Client
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy the Python script
COPY els.py /app/els.py

# Make the script executable
RUN chmod +x /app/els.py

# Create data directory
RUN mkdir -p /data

# Set the data directory as a volume mount point
VOLUME ["/data"]

# Install any additional Python packages if needed
RUN pip install -r requirments.txt

# Run the Python script
CMD ["python3", "/app/els.py"]
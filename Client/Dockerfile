# Dockerfile for ELS Client
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy the Python script
COPY els.py /app/els.py

# Make the script executable
RUN chmod +x els.py

# Create data directory
RUN mkdir -p /data

# Set the data directory as a volume mount point
VOLUME ["/data"]

# Install any additional Python packages if needed
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

# Run the Python script
CMD ["python3", "els.py"]
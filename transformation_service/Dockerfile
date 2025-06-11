FROM python:3.10-slim

# Install Java, procps, and system dependencies
RUN apt-get update && apt-get install -y openjdk-17-jdk-headless procps && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Run transformation script
CMD ["python", "transformation.py"]

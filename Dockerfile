FROM bitnami/spark:3.5.0

WORKDIR /app

# Add required tools and Python
USER root
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install -y python3-pip python3-venv

# Copy your code into the container
COPY . .

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Set default command
CMD ["spark-submit", "--master", "local[*]", "data_transformation.py"]

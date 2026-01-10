# Use official Spark image
FROM apache/spark:3.5.0

USER root

# Install Python deps
# The official image is based on Java, we need to ensure python/pip is there.
# It usually has python3. Let's try installing pip via apt-get if missing or ensure we use pip3.
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir requests pandas psycopg2-binary

WORKDIR /app
COPY controller.py .

CMD ["python3", "controller.py"]

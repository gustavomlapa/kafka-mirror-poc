FROM python:3.11-slim

# Install librdkafka dependencies and certificates
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    librdkafka-dev \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install Python dependencies
COPY apps/producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY apps/ /app/

ENV PYTHONUNBUFFERED=1
ENV PORT=8080

EXPOSE 8080

CMD ["python", "producer/producer.py"]

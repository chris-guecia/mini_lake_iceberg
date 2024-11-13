# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python script
COPY simulate_ingestion_elt.py .

CMD ["tail", "-f", "/dev/null"]
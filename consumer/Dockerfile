FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .
COPY wait_for_kafka.py .

ENTRYPOINT ["python", "wait_for_kafka.py"]

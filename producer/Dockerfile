FROM python:3.11

WORKDIR /app

ENV PYTHONPATH=/app

# Copy only what is needed
COPY producer/ ./producer/
COPY shared/ ./shared/

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ["python", "producer/producer.py"]

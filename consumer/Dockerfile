FROM python:3.11

WORKDIR /app

ENV PYTHONPATH=/app

# Copy project parts
COPY consumer/ ./consumer/
COPY lsh/ ./lsh/
COPY database/ ./database/
COPY shared/ ./shared/

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ["python", "consumer/consumer.py"]

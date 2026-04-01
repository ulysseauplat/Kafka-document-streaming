FROM python:3.11

WORKDIR /app

ENV PYTHONPATH=/app

COPY producer/ ./producer/
COPY shared/ ./shared/

COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ["python", "producer/main.py"]

FROM python:3.11

WORKDIR /app

ENV PYTHONPATH=/app

COPY consumer/ ./consumer/
COPY lsh/ ./lsh/
COPY database/ ./database/
COPY shared/ ./shared/

COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ["python", "-c", "from dotenv import load_dotenv; load_dotenv(); from consumer.consumer import main; main()"]

FROM python:3.11-slim

WORKDIR /app

COPY flask-ui/ ./flask-ui/

RUN pip install --no-cache-dir -r flask-ui/requirements.txt

ENV PYTHONPATH=/app

EXPOSE 5000

CMD ["python", "flask-ui/app.py"]

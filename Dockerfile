FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY mocks ./mocks
COPY templates ./templates

EXPOSE 8000 9000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

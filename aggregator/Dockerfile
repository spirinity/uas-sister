FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN addgroup --system app && adduser --system --ingroup app app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY aggregator_app ./aggregator_app

USER app

EXPOSE 8080

CMD ["sh", "-c", "exec uvicorn aggregator_app.main:app --host 0.0.0.0 --port 8080 --workers ${APP_WORKERS:-4}"]

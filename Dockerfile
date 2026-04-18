FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/ ./services/
COPY tests/ ./tests/

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "services.main:app", "--host", "0.0.0.0", "--port", "8000"]

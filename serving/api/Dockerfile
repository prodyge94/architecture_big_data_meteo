FROM python:3.11-slim
RUN pip install --no-cache-dir fastapi==0.115.2 "uvicorn[standard]==0.32.0" psycopg2-binary==2.9.10
WORKDIR /app
COPY main.py .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM python:3.11-slim
RUN pip install --no-cache-dir streamlit==1.39.0 pandas==2.2.3 psycopg2-binary==2.9.10
WORKDIR /app
COPY app.py .
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0", "--browser.gatherUsageStats=false"]

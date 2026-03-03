FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY meteoro_api.py .
COPY meteoro_pipeline.py .
COPY latency_benchmark.py .
COPY meteoro_app.html .
COPY data_sources/ ./data_sources/

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health')"

# Run server
CMD ["uvicorn", "meteoro_api:app", "--host", "0.0.0.0", "--port", "8000"]

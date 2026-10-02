# Production Dockerfile for OpsPulse Alexa+ MCP Server
FROM python:3.12-slim

# Prevent bytecode compilation and enable unbuffered output for container logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000 \
    HOST=0.0.0.0

WORKDIR /app

# Install lightweight system dependencies (curl for container health probes)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python production dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code into container
COPY server.py index.html README.md test_client.py .env.example ./

# Expose the MCP Streamable HTTP / SSE port
EXPOSE 8000

# Docker native liveness probe
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Launch ASGI application
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]

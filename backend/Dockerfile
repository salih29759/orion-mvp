FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user for security
RUN adduser --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

# Cloud Run expects the app to listen on $PORT (default 8080)
ENV PORT=8080

EXPOSE 8080

# Use shell form so $PORT is expanded at runtime
CMD ["./start.sh"]

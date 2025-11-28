# Use Python 3.13 to match your local version
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Accept build arguments for user ID and group ID
ARG UID
ARG GID

# Install system dependencies and create user
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && rm -rf /var/lib/apt/lists/* \
    && (getent group $GID || addgroup --gid $GID appuser) \
    && adduser --disabled-password --gecos "" --uid $UID --gid $GID appuser

# Set timezone
ENV TZ=Europe/Athens
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Copy requirements first (for Docker layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy project structure
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY queries/ ./queries/
COPY media/ ./media/
COPY tests/ ./tests/

# Create necessary directories and set ownership
RUN mkdir -p logs data && chown -R $UID:$GID /app

# Set Python to run in unbuffered mode (see logs in real-time)
ENV PYTHONUNBUFFERED=1

# Switch to non-root user
USER appuser

# Run with scheduling enabled by default using new main.py entry point
CMD ["python", "-m", "src.main"]

# Healthcheck to monitor container
HEALTHCHECK --interval=1h --timeout=10s --start-period=30s --retries=3 \
    CMD test -f /app/logs/alerts.log && \
        MINUTES=$(python3 -c "import os; print(int(float(os.getenv('SCHEDULE_FREQUENCY_HOURS', '1')) * 60 + 10))") && \
        test $(find /app/logs/alerts.log -mmin -${MINUTES} | wc -l) -eq 1 || exit 1

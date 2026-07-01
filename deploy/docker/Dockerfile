# deploy/docker/Dockerfile
# Stage 1: Build dependencies
FROM python:3.13-slim AS builder

WORKDIR /app

# Install build essentials
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Pre-install core python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Stage 2: Runtime image
FROM python:3.13-slim AS runner

WORKDIR /app

# Copy installed libraries from builder stage
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1

# Copy source code files
COPY inferx/ ./inferx/

# Create a non-privileged system user for execution safety
RUN groupadd -g 10001 inferx && \
    useradd -u 10001 -g inferx -s /bin/sh -m inferx && \
    chown -R inferx:inferx /app

USER inferx

EXPOSE 8080

ENTRYPOINT ["python", "-m", "inferx.main"]

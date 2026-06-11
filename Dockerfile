# Use a lightweight Python image
FROM python:3.12-slim

# Install system dependencies required for psycopg (Postgres adapter)
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# Install uv for blazingly fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set the working directory
WORKDIR /app

# Copy dependency files and install them
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-cache

# Copy your application code
COPY main.py .
COPY .env .

# Expose the web server port
EXPOSE 8000

# Start the FastAPI Uvicorn server using the uv virtual environment
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
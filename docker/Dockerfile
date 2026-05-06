# Dockerfile
FROM python:3.12-slim

# prevent python from write .pyc files and ensure logs flush immediately
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy application code
COPY app ./app

# default command
CMD ["python", "-m", "app.main"]
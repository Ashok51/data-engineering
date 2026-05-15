FROM python:3.12-slim

# it means all the commands will be run from the /app directory or this is root directory of container.
WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

CMD ["python", "-m", "src.main"]
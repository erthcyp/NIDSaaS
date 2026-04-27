FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py cascade.py ./

# Research pipeline is bind-mounted at /opt/pipeline by docker-compose.
ENV PYTHONPATH="/app:/opt/pipeline"
ENV PYTHONUNBUFFERED=1

CMD ["python", "worker.py"]

FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
ENV PODRIFT_DB=/data/podrift.db
EXPOSE 12100
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "12100"]

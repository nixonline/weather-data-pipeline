FROM python:3.11-slim

WORKDIR /app/transformer
COPY transformer/ /app/transformer/
COPY shared/ /app/shared/

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "weather_transformer.py"]

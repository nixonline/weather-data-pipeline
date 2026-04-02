FROM python:3.11-slim

WORKDIR /app/extractor
COPY extractor/ /app/extractor/
COPY shared/ /app/shared/

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "weather_extractor.py"]

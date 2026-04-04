FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app/extractor

COPY extractor/ /app/extractor/
COPY shared/ /app/shared/

RUN pip install --no-compile -r requirements.txt

ENTRYPOINT ["python", "/app/extractor/weather_extractor.py"]

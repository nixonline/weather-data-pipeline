FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app/transformer

COPY transformer/ /app/transformer/
COPY shared/ /app/shared/

RUN pip install --no-compile -r requirements.txt

ENTRYPOINT ["python", "/app/transformer/weather_transformer.py"]

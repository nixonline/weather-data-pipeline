FROM python:3.11-slim

WORKDIR /app/transformer
COPY transformer/ /app/transformer/
COPY shared/ /app/shared/

CMD ["python", "transform.py"]

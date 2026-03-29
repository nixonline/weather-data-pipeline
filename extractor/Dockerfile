FROM python:3.11-slim

WORKDIR /app
COPY . .

RUN pip install pandas python-dotenv

CMD ["python", "extract.py"]
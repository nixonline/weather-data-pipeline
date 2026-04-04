# Local Kestra Setup

## Build the app images

From the project root:

```powershell
docker build -t extractor:latest -f extractor/Dockerfile .
docker build -t transformer:latest -f transformer/Dockerfile .
```

## Start Kestra

From the `orchestration` folder:

```powershell
docker compose --env-file ../.env up -d
```

Open `http://localhost:8080`.

## Run the flow

In the Kestra UI, run:

- Namespace: `dev`
- Flow: `weather-data-pipeline`

Kestra will pass `GCS_BUCKET` and `GCP_SERVICE_KEY` from your root `.env` into both containers.

## Notes

- This setup is for local development only.
- It uses Kestra's local mode with embedded H2 storage.
- The flow file is stored in `orchestration/flows/` so Kestra can watch and load it locally.

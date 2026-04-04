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

## Local and production flows

Two flow variants are available:

- `dev.weather-data-pipeline`
  - Uses local Docker images: `extractor:latest` and `transformer:latest`
- `prod.weather-data-pipeline`
  - Uses image names provided by Kestra environment variables:
    - `ENV_EXTRACTOR_IMAGE`
    - `ENV_TRANSFORMER_IMAGE`

For a deployed VM, set those environment variables to your Artifact Registry image URLs.
In the VM `.env`, define:

```env
EXTRACTOR_IMAGE=asia-southeast1-docker.pkg.dev/<project>/<repo>/extractor:latest
TRANSFORMER_IMAGE=asia-southeast1-docker.pkg.dev/<project>/<repo>/transformer:latest
GCS_BUCKET=your-bucket
GCP_SERVICE_KEY={"type":"service_account",...}
```

## Notes

- This setup is for local development only.
- It uses Kestra's local mode with embedded H2 storage.
- The flow file is stored in `orchestration/flows/` so Kestra can watch and load it locally.

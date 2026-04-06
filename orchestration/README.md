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

Flow variants are available for both full-pipeline and demo runs:

- `dev.weather-data-pipeline`
  - Uses local Docker images: `extractor:latest` and `transformer:latest`
- `dev.extract-only`
  - Runs only the extractor with local Docker image `extractor:latest`
- `dev.transform-only`
  - Runs only the transformer with local Docker image `transformer:latest`
- `prod.weather-data-pipeline`
  - Uses image names provided by Kestra environment variables:
    - `ENV_EXTRACTOR_IMAGE`
    - `ENV_TRANSFORMER_IMAGE`
- `prod.extract-only`
  - Runs only the extractor using the configured Artifact Registry image
- `prod.transform-only`
  - Runs only the transformer using the configured Artifact Registry image

For a deployed VM, set those environment variables to your Artifact Registry image URLs.
In the VM `.env`, define:

```env
EXTRACTOR_IMAGE=asia-southeast1-docker.pkg.dev/<project>/<repo>/extractor:latest
TRANSFORMER_IMAGE=asia-southeast1-docker.pkg.dev/<project>/<repo>/transformer:latest
GCS_BUCKET=your-bucket
GCP_SERVICE_KEY={"type":"service_account",...}
KESTRA_ADMIN_EMAIL=admin@yourdomain.com
KESTRA_ADMIN_PASSWORD=replace-with-a-strong-password
KESTRA_POSTGRES_DB=kestra
KESTRA_POSTGRES_USER=kestra
KESTRA_POSTGRES_PASSWORD=replace-with-a-strong-password
```

## Notes

- Kestra now uses local Postgres for repository and queue persistence, so users, auth, and schedules survive container restarts on the VM.
- Flow storage still uses a local Docker volume mounted at `/app/storage`.
- The flow file is stored in `orchestration/flows/` so Kestra can watch and load it locally.

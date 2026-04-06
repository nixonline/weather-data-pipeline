# Weather Data Pipeline

This project is a batch data engineering pipeline for collecting daily weather forecasts for selected Southeast Asian cities using the Open-Meteo API, storing the raw files in Google Cloud Storage, transforming them into an analytics-ready dataset, and loading the results into BigQuery.

On a high level, this app:

- extracting forecast data from the Open-Meteo API for a curated set of cities
- storing raw batch files in GCS as a simple data lake layer
- transforming and deduplicating the data into an analytics-ready table
- loading the transformed data into BigQuery for downstream querying
- orchestrating the whole flow with Kestra

The pipeline keeps the most recent forecast for each city and date, and it also computes rolling temperature averages that make trend analysis easier.

## Architecture

The batch pipeline follows this flow:

1. `extractor` calls the Open-Meteo API and writes batch CSV files.
2. Raw files are uploaded to `GCS` under the `daily/` prefix.
3. `transformer` downloads raw weather files from GCS, cleans and enriches them, and upserts the results into `BigQuery`.
4. Kestra orchestrates extract, transform, and supporting SQL tasks.
5. Kestra itself runs on a GCE VM with local Postgres for metadata persistence.

Main cloud services used:

- `Google Compute Engine` for the Kestra VM
- `Artifact Registry` for Docker images
- `Google Cloud Storage` for raw files and logs
- `BigQuery` for the data warehouse
- `GitHub Actions` for image build and push automation

## Dataset and Output

Source:

- `Open-Meteo API`

Coverage in the current config:

- Philippines
- Singapore
- Malaysia
- Thailand
- Vietnam

Main outputs:

- Raw weather CSV files in `GCS`
- Transformed weather table in `BigQuery`
- Date dimension-style helper table in `BigQuery`
- Run logs in `GCS` and BigQuery logs table

## Workflow Orchestration

Kestra flow files live in `orchestration/flows`.

Main flows:

- `dev.weather-data-pipeline`
- `prod.weather-data-pipeline`

Demo-friendly flows:

- `dev.weather-data-extract`
- `dev.transform-only`
- `prod.extract-only`
- `prod.transform-only`

The production flow uses Artifact Registry images and explicit registry credentials so Kestra can pull images from GCP.

## Dashboard

Looker Studio dashboard:

- https://lookerstudio.google.com/reporting/02cb8f80-1141-409c-bff4-5266a9a19169


## How to Run

### 1. Run Locally

#### Prerequisites

- Docker Desktop or Docker Engine
- Docker Compose
- a GCS bucket
- a GCP service account key with access to GCS and BigQuery

#### Step 1: create a root `.env`

Create a `.env` file in the project root:

```env
GCS_BUCKET=<your-gcs-bucket>
GCP_SERVICE_KEY={"type":"service_account",...}
EXTRACTOR_IMAGE=extractor:latest
TRANSFORMER_IMAGE=transformer:latest
KESTRA_ADMIN_EMAIL=<admin-email>
KESTRA_ADMIN_PASSWORD=replace-with-a-strong-password
KESTRA_POSTGRES_DB=kestra
KESTRA_POSTGRES_USER=kestra
KESTRA_POSTGRES_PASSWORD=replace-with-a-strong-password
```

#### Step 2: build the local images

From the project root:

```powershell
docker build -t extractor:latest -f extractor/Dockerfile .
docker build -t transformer:latest -f transformer/Dockerfile .
```

#### Step 3: start Kestra locally

```powershell
cd orchestration
docker compose --env-file ../.env up -d
```

#### Step 4: open the UI

Open:

```text
http://localhost:8080
```

Log in with:

- `KESTRA_ADMIN_EMAIL`
- `KESTRA_ADMIN_PASSWORD`

#### Step 5: run a dev flow

In the Kestra UI, use the `dev` namespace and run one of:

- `dev.weather-data-pipeline`
- `dev.weather-extract`
- `dev.weather-transform`

### 2. Deploy to Production

#### Step 1: build and push the images

This repo includes a GitHub Actions workflow that publishes:

- `extractor:latest`
- `transformer:latest`

to Artifact Registry.

Push to `main` or manually run the workflow in GitHub Actions.

#### Step 2: create the VM

Create a GCE VM for Kestra. The current working setup uses:

- machine type: `e2-medium`
- Ubuntu
- Docker and Docker Compose installed on the VM

The VM service account should have at least:

- `Artifact Registry Reader`

#### Step 3: clone the repo on the VM

```bash
git clone https://github.com/nixonline/weather-data-pipeline.git
cd ~/weather-data-pipeline
```

#### Step 4: create the VM `.env`

Create a `.env` file in the repo root on the VM:

```env
EXTRACTOR_IMAGE=<artifact-registry-image-url-for-extractor>
TRANSFORMER_IMAGE=<artifact-registry-image-url-for-transformer>
GCS_BUCKET=<your-gcs-bucket>
GCP_SERVICE_KEY={"type":"service_account",...}
KESTRA_ADMIN_EMAIL=<admin-email>
KESTRA_ADMIN_PASSWORD=replace-with-a-strong-password
KESTRA_POSTGRES_DB=kestra
KESTRA_POSTGRES_USER=kestra
KESTRA_POSTGRES_PASSWORD=replace-with-a-strong-password
```

#### Step 5: start Kestra on the VM

```bash
cd ~/weather-data-pipeline/orchestration
docker compose --env-file ../.env up -d
```

#### Step 6: open Kestra

Open:

```text
http://<VM_EXTERNAL_IP>:8080
```

Log in with:

- `KESTRA_ADMIN_EMAIL`
- `KESTRA_ADMIN_PASSWORD`

#### Step 7: run a prod flow

In the Kestra UI, use the `prod` namespace and run one of:

- `prod.weather-data-pipeline`
- `prod.weather-extract`
- `prod.weather-transform`

#### Step 8: update the deployment

When the branch gets new changes:

1. push the latest code to GitHub
2. let GitHub Actions publish the updated Docker images
3. on the VM, pull the latest repo changes:

```bash
cd ~/weather-data-pipeline
git pull
```

4. restart Kestra so flow changes reload:

```bash
cd ~/weather-data-pipeline/orchestration
docker compose --env-file ../.env restart kestra
```

## Future Improvements

- add BigQuery partitioning and clustering
- replace Pandas with Polars

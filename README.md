# TS-Arena Backend



This repository serves as a sub-component containing the backend infrastructure, including:
- **API-Portal**
- **Data-Portal**
- **Dashboard-API**
- **TimescaleDB Database**

These services work together within a Docker network to power the platform.

## Concept

TS-Arena is a platform designed for **pre-registered forecasts into the real future**. This approach ensures rigorous, information leakage-free evaluations by comparing model predictions against data that did not exist at the time of the forecast.

This repository contains the backend infrastructure that powers the TS-Arena platform. It is designed to be self-hostable, allowing you to run your own instance of the benchmarking environment.

## Architecture

The backend consists of three main microservices that work together:

*   **`data-portal`**: This service is responsible for fetching ground truth data from external APIs (e.g., energy, air quality) for the benchmarks. It processes and stores this data in a structured format within a TimescaleDB database.
*   **`api-portal`**: This service orchestrates the forecasting challenges. It handles the registration of models, accepts incoming forecasts, and manages the evaluation process against the ground truth data collected by the data-portal.
*   **`dashboard-api`**: This API serves the frontend application. It retrieves relevant statistics, leaderboard data, and challenge information from the database to be displayed in the user interface.

## Integrated Data Sources

Currently, the following data sources are integrated and active:

### EIA (U.S. Energy Information Administration)
The data used in this platform are obtained from the [U.S. Energy Information Administration (EIA) Data Portal](https://www.eia.gov/opendata/). The data are accessed for non-commercial, scientific purposes and are used only partially. The EIA is acknowledged as the original source of the data.

### SMARD (Bundesnetzagentur)
The data used in this platform are obtained from the [SMARD electricity and gas market data platform](https://www.smard.de/home) operated by the German Federal Network Agency (Bundesnetzagentur). The data, licensed under the Creative Commons Attribution 4.0 International Licence (CC BY 4.0), are accessed and partially used for non-commercial, scientific purposes. The original source of the data is acknowledged as "Bundesnetzagentur | SMARD.de", and any modifications or aggregations of the data are the sole responsibility of the authors.

### Fingrid (Finnish Transmission System Operator)
The data used in this platform are obtained from [Fingrid's Open Data Platform](https://data.fingrid.fi/en/). The data, licensed under the [Creative Commons Attribution 4.0 International License (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/), are accessed and partially used for non-commercial, scientific purposes. Source: Fingrid / data.fingrid.fi. Any modifications or aggregations of the data are the sole responsibility of the authors. For full license terms, see [Fingrid's Legal Terms](https://www.fingrid.fi/en/legal-terms/).

## Getting Started (Self-Hosting)

To host the TS-Arena backend yourself, you primarily need Docker and Docker Compose.

### 1. Configuration

Create a `.env` file in the root directory of the project. You can use the variables defined in `docker-compose.yml` as a reference. Below is a list of key environment variables you will need to configure:

**Database Configuration:**
*   `POSTGRES_USER`: Username for the TimescaleDB instance.
*   `POSTGRES_PASSWORD`: Password for the TimescaleDB instance.
*   `POSTGRES_DB`: Name of the database.
*   `DATABASE_URL`: Connection string for the services to access the database (e.g., `postgresql://user:password@timescaledb:5432/dbname`).

**API Keys & Security:**
*   `API_KEY`: Master API key for the `api-portal`.
*   `DASHBOARD_API_KEY`: API key for the `dashboard-api`.

**Data Sources (Optional, depending on enabled plugins):**
*   `API_KEY_SOURCE_EIA`: API key for EIA data source.
*   `API_KEY_SOURCE_OPENAQ`: API key for OpenAQ data source.

**Other Settings:**
*   `DASHBOARD_API_URL`: URL where the dashboard API is accessible.
*   `DEBUG`: Set to `true` or `false`.
*   `LOG_LEVEL`: Logging level (e.g., `INFO`).
*   `SCHEDULER_TIMEZONE`: Timezone for the scheduler (default: `UTC`).

### 2. Run the Application

Once your `.env` file is set up, you can start the entire stack using Docker Compose:

```bash
docker compose up -d
```

This command will pull the necessary images (or build them), initialize the database, and start all services.

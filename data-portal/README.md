# Data Portal Service

The Data Portal service is responsible for automatically fetching time series data from configured external sources and storing them in TimescaleDB. It runs as a standalone microservice with scheduled jobs.

## Architecture

- **APScheduler**: Background scheduler with SQLAlchemy job store for persistence
- **Plugin System**: Modular data source plugins loaded from YAML configuration
- **TimescaleDB**: Efficient time series data storage with hypertables
- **FastAPI**: REST API for health checks and monitoring

## Features

- **Automated Data Collection**: Scheduled jobs fetch data at configured intervals
- **Retry Logic**: Exponential backoff for transient API failures
- **Persistent Jobs**: Job state survives container restarts
- **Concurrent Prevention**: Max 1 instance per job to prevent overlaps
- **Health Monitoring**: REST endpoints for status checks
- **SCD Type 2 History Tracking**: Complete audit trail of data changes with point-in-time queries

## Configuration

### Environment Variables

- `DATABASE_URL`: PostgreSQL/TimescaleDB connection string
- `LOG_LEVEL`: Logging level (default: INFO)
- `SCHEDULER_TIMEZONE`: Scheduler timezone (default: UTC)
- `PLUGIN_CONFIG_PATH`: Path to sources.yaml (default: src/plugins/configs/sources.yaml)
- `MAX_RETRIES`: Maximum retry attempts for failed fetches (default: 3)
- `RETRY_DELAY_SECONDS`: Initial retry delay in seconds (default: 60)

### Plugin Configuration

Plugins are configured in `src/plugins/configs/sources.yaml`:

```yaml
timeseries:
  smard-1223-de-hour:
    module: src.plugins.data_sources.smard_plugin
    class: SmardDataSourcePlugin
    metadata:
      name: SMARD-1223-DE-hour
      description: 'Power Generation: Brown Coal - Country: Germany'
      granularity: 1 hour
      forecast_horizon: 1 day
      available_metrics:
        - power
      update_frequency: 1 hour  # Schedule interval
    default_params:
      filter: 1223
      region: DE
      resolution: hour
```

## API Endpoints

- `GET /` - Service information
- `GET /health` - Health check
- `GET /jobs` - List all scheduled jobs

## Data Flow

1. **Plugin Loading**: On startup, load all plugins from sources.yaml
2. **Job Registration**: Register scheduled jobs based on `update_frequency`
3. **Data Fetching**: Jobs execute at scheduled intervals:
   - Fetch latest timestamp from database
   - Call plugin's `get_historical_data()` method
   - Retry with exponential backoff on failure
4. **Data Storage**: Upsert data points to `time_series_data` table
5. **Error Handling**: Log errors, continue with next scheduled run

## Database Schema

### Tables Used

- `time_series`: Metadata for each data source
- `time_series_data`: Hypertable for current time series data points
- `time_series_data_scd2`: SCD Type 2 hypertable with complete version history

### Data Model

```sql
-- Metadata
INSERT INTO time_series (name, endpoint_prefix, granularity, ...)
VALUES ('SMARD-1223-DE-hour', 'smard-1223-de-hour', '1 hour', ...);

-- Data points (upsert)
INSERT INTO time_series_data (series_id, ts, value, updated_at)
VALUES (1, '2025-01-01 00:00:00+00', 12345.6, NOW())
ON CONFLICT (series_id, ts) DO UPDATE
SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;
```

## Development

### Local Setup

```bash
cd data-portal
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Running Locally

```bash
export DATABASE_URL="postgresql+asyncpg://user:pass@localhost:5432/tsfm_arena"
python -m uvicorn src.main:app --reload
```

### Adding New Data Sources

1. Create plugin class in `src/plugins/data_sources/`
2. Inherit from `BasePlugin` and implement `get_historical_data()`
3. Add configuration to `sources.yaml`
4. Restart service to load new plugin

## Docker Deployment

```bash
docker build -t data-portal .
docker run -p 8000:8000 --env-file .env data-portal
```

## Monitoring

- Check job status: `curl http://localhost:8000/jobs`
- View logs: `docker logs data-portal`
- Database queries:
  ```sql
  SELECT * FROM time_series;
  SELECT series_id, COUNT(*), MAX(timestamp) 
  FROM time_series_data 
  GROUP BY series_id;
  ```

## Robustness Features

- **Error Isolation**: Plugin failures don't affect other jobs
- **Graceful Shutdown**: SIGTERM handling ensures clean shutdown
- **Connection Pooling**: SQLAlchemy manages database connections
- **Misfire Handling**: Missed jobs coalesced and executed with grace time
- **Logging**: Structured logging for debugging and monitoring
- **Data Versioning**: SCD Type 2 tracks all data changes with temporal validity
- **Audit Trail**: Complete history of when and how data values changed

## Additional Documentation

- [SCD2_INTEGRATION.md](SCD2_INTEGRATION.md) - Detailed documentation on SCD Type 2 history tracking

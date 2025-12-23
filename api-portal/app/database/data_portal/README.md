# Time Series Repository - Usage Guide

## Overview

The `TimeSeriesRepository` provides comprehensive read-only access to time series data with support for:
- Single and bulk time series queries
- Time range-based queries
- Last N points queries
- Copy functions to challenge context data

## Basic Usage

```python
from app.database.data_portal.time_series_repository import TimeSeriesRepository
from app.database.connection import SessionLocal
from datetime import datetime, timedelta

async with SessionLocal() as session:
    repo = TimeSeriesRepository(session)
    
    # Get metadata
    series = await repo.get_time_series_by_name("electricity_price_de")
```

## Query Methods

### 1. Single Time Series - By Time Range

```python
# Get data points within a specific time range
start = datetime(2025, 1, 1)
end = datetime(2025, 1, 31)

data = await repo.get_data_by_time_range(
    series_id=1,
    start_time=start,
    end_time=end
)
# Returns: [{"ts": datetime(...), "value": 123.45}, ...]
```

### 2. Single Time Series - Last N Points

```python
# Get the last 1000 data points
data = await repo.get_last_n_points(
    series_id=1,
    n=1000
)

# Get last 1000 points before a specific time
cutoff = datetime(2025, 10, 1)
data = await repo.get_last_n_points(
    series_id=1,
    n=1000,
    before_time=cutoff
)
```

### 3. Bulk Time Series - By Time Range

```python
# Get data for multiple time series at once
series_ids = [1, 2, 3, 4, 5]
start = datetime(2025, 1, 1)
end = datetime(2025, 1, 31)

data_by_series = await repo.get_bulk_data_by_time_range(
    series_ids=series_ids,
    start_time=start,
    end_time=end
)
# Returns: {
#   1: [{"ts": ..., "value": ...}, ...],
#   2: [{"ts": ..., "value": ...}, ...],
#   ...
# }
```

### 4. Bulk Time Series - Last N Points

```python
# Get last 500 points for multiple series
series_ids = [1, 2, 3, 4, 5]

data_by_series = await repo.get_bulk_last_n_points(
    series_ids=series_ids,
    n=500
)
```

## Copy Functions to Challenge Context Data

### 1. Copy Last N Points

```python
# Copy the last 1000 points from a time series to challenge context
count = await repo.copy_last_n_to_challenge(
    series_id=1,
    series_name="electricity_price",  # Identifier in challenge
    challenge_id=5,
    n=1000
)
print(f"Copied {count} data points")
```

### 2. Copy Time Range

```python
# Copy data from a specific time range
count = await repo.copy_time_range_to_challenge(
    series_id=1,
    series_name="electricity_price",
    challenge_id=5,
    start_time=datetime(2025, 1, 1),
    end_time=datetime(2025, 3, 31)
)
```

### 3. Bulk Copy Multiple Series

```python
# Copy data from multiple series to a challenge
series_mapping = {
    1: "electricity_price",
    2: "solar_production",
    3: "wind_production",
    4: "demand_total"
}

# Option A: Copy last N points for each series
result = await repo.copy_bulk_to_challenge(
    series_mapping=series_mapping,
    challenge_id=5,
    n=1000
)
# Returns: {1: 1000, 2: 1000, 3: 1000, 4: 1000}

# Option B: Copy time range for each series
result = await repo.copy_bulk_to_challenge(
    series_mapping=series_mapping,
    challenge_id=5,
    start_time=datetime(2025, 1, 1),
    end_time=datetime(2025, 3, 31)
)
```

## Complete Example: Preparing Challenge Context Data

```python
from app.database.data_portal.time_series_repository import TimeSeriesRepository
from app.database.connection import SessionLocal
from datetime import datetime, timedelta

async def prepare_challenge_context(challenge_id: int):
    """
    Example function that prepares context data for a challenge.
    """
    async with SessionLocal() as session:
        repo = TimeSeriesRepository(session)
        
        # Define which time series to include
        series_config = {
            "electricity_price_de": "price",
            "solar_production_de": "solar",
            "wind_production_de": "wind",
            "electricity_demand_de": "demand"
        }
        
        # Get series IDs
        series_mapping = {}
        for series_name, challenge_name in series_config.items():
            series = await repo.get_time_series_by_name(series_name)
            if series:
                series_mapping[series.series_id] = challenge_name
        
        # Copy last 10,000 points for each series
        result = await repo.copy_bulk_to_challenge(
            series_mapping=series_mapping,
            challenge_id=challenge_id,
            n=10000
        )
        
        # Commit the transaction
        await session.commit()
        
        total_points = sum(result.values())
        print(f"Prepared challenge {challenge_id} with {total_points} context data points")
        print(f"Breakdown: {result}")

# Usage
await prepare_challenge_context(challenge_id=5)
```

## Performance Considerations

1. **Bulk Operations**: Use bulk methods when querying multiple series to reduce database round-trips
2. **Copy Functions**: Use `ON CONFLICT DO NOTHING` to safely handle duplicate data
3. **TimescaleDB**: All queries are optimized for TimescaleDB hypertables
4. **Indexing**: Queries leverage composite indexes on (series_id, ts)

## Error Handling

All methods log errors and re-raise exceptions. Recommended usage:

```python
try:
    data = await repo.get_last_n_points(series_id=1, n=1000)
except Exception as e:
    logger.error(f"Failed to retrieve data: {e}")
    # Handle error appropriately
```

## Integration with Challenge Service

Typical workflow in a challenge service:

```python
from app.database.data_portal.time_series_repository import TimeSeriesRepository
from app.database.challenges.challenge_repository import ChallengeRepository

async def create_challenge_with_context(challenge_data: dict, series_list: list):
    async with SessionLocal() as session:
        # Create challenge
        challenge_repo = ChallengeRepository(session)
        challenge = await challenge_repo.create_challenge(challenge_data)
        
        # Prepare context data
        ts_repo = TimeSeriesRepository(session)
        series_mapping = {}
        for series_info in series_list:
            series = await ts_repo.get_time_series_by_name(series_info["name"])
            if series:
                series_mapping[series.series_id] = series_info["challenge_name"]
        
        # Copy data based on challenge dates
        await ts_repo.copy_bulk_to_challenge(
            series_mapping=series_mapping,
            challenge_id=challenge.id,
            start_time=challenge.context_start,
            end_time=challenge.context_end
        )
        
        await session.commit()
        return challenge
```

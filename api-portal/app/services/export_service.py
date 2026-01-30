import io
import zipfile
import logging
from datetime import datetime, timezone
import pandas as pd
from typing import List, Dict, Any, Optional
from sqlalchemy import extract, select, and_

from app.services.challenge_service import ChallengeService
from app.database.challenges.challenge import ChallengeRound
from app.database.challenges.challenge_repository import ChallengeRoundRepository

logger = logging.getLogger(__name__)

class ExportService:
    def __init__(self, db_session, challenge_service: ChallengeService):
        self.db_session = db_session
        self.challenge_service = challenge_service
        self.round_repository = ChallengeRoundRepository(db_session)

    async def export_monthly_data(self, year: int, month: int, definition_id: Optional[int] = None) -> io.BytesIO:
        """
        Exports all challenge data for a specific month as a ZIP of Parquet files.
        Rounds are selected based on their end_time falling within the month.
        """
        logger.info(f"Starting export for {year}-{month:02d} (definition_id={definition_id})")
        
        # 1. Fetch relevant rounds
        # We can't easily access the repository from here if it's not passed, 
        # but we initialized it.
        # Filter rounds where end_time is in the given month and year
        conditions = [
            extract('year', ChallengeRound.end_time) == year,
            extract('month', ChallengeRound.end_time) == month
        ]
        if definition_id is not None:
            conditions.append(ChallengeRound.definition_id == definition_id)
            
        query = select(ChallengeRound).where(and_(*conditions))
        result = await self.db_session.execute(query)
        rounds = result.scalars().all()
        
        if not rounds:
            logger.warning(f"No rounds found for {year}-{month:02d}")
            # Return empty zip or raise? Let's return empty zip with readme
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zf:
                zf.writestr("README.txt", f"No rounds found for {year}-{month:02d}")
            zip_buffer.seek(0)
            return zip_buffer

        logger.info(f"Found {len(rounds)} rounds to export.")

        # Containers for data
        rounds_metadata = []
        all_context = []
        all_actuals = []
        all_forecasts = []

        # 2. Iterate and fetch data
        for r in rounds:
            rounds_metadata.append({
                "round_id": r.id,
                "name": r.name,
                "definition_id": r.definition_id,
                "status": r.status,
                "start_time": r.start_time,
                "end_time": r.end_time,
                "registration_start": r.registration_start,
                "created_at": r.created_at
            })
            
            # Use the existing efficient Time Travel query
            # We get raw data to avoid Pydantic overhead and easier flattening
            round_data_raw = await self.challenge_service.round_repository.get_round_complete_data(r.id)
            
            series_data_list = round_data_raw.get("series_data", [])
            for s_data in series_data_list:
                series_id = s_data["series_id"]
                series_name = s_data["challenge_series_name"]
                
                # Context
                for pt in s_data["context"]:
                    all_context.append({
                        "round_id": r.id,
                        "series_id": series_id,
                        "challenge_series_name": series_name,
                        "ts": pt["ts"],
                        "value": pt["value"]
                    })
                
                # Actuals
                for pt in s_data["actuals"]:
                    all_actuals.append({
                        "round_id": r.id,
                        "series_id": series_id,
                        "challenge_series_name": series_name,
                        "ts": pt["ts"],
                        "value": pt["value"]
                    })
                
                # Forecasts (dict: readable_id -> list of points)
                f_data = s_data["forecasts"]
                for readable_id, points in f_data.items():
                    # readable_id is now a string thanks to repo update
                    for pt in points:
                        all_forecasts.append({
                            "round_id": r.id,
                            "series_id": series_id,
                            "challenge_series_name": series_name,
                            "readable_id": readable_id,
                            "ts": pt["ts"],
                            "value": pt["value"]
                        })

        # 3. Convert to DataFrames and Parquet
        logger.info("Converting to DataFrames...")
        
        df_rounds = pd.DataFrame(rounds_metadata)
        df_context = pd.DataFrame(all_context)
        df_actuals = pd.DataFrame(all_actuals)
        df_forecasts = pd.DataFrame(all_forecasts)
        
        # 4. create Zip file in memory
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zf:
            
            # Helper to write DF to parquet in zip
            def write_df_to_zip(df, filename):
                if not df.empty:
                    # To write to zip, we need another buffer or write to temp file
                    # PyArrow can write to BytesIO
                    with io.BytesIO() as pq_buffer:
                        df.to_parquet(pq_buffer, engine="pyarrow", index=False)
                        zf.writestr(filename, pq_buffer.getvalue())
                else:
                    # Write empty file marker or readme?
                    zf.writestr(f"{filename}.empty", "No data")

            write_df_to_zip(df_rounds, "rounds_metadata.parquet")
            write_df_to_zip(df_context, "context.parquet")
            write_df_to_zip(df_actuals, "actuals.parquet")
            write_df_to_zip(df_forecasts, "forecasts.parquet")
            
            zf.writestr("README.txt", f"Export generated at {datetime.now(timezone.utc)}\nFilter: Year={year}, Month={month}, DefinitionID={definition_id}")

        zip_buffer.seek(0)
        logger.info("Export complete.")
        return zip_buffer

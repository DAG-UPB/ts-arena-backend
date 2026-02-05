import asyncio
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone, timedelta, date
from dataclasses import dataclass
import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

logger = logging.getLogger(__name__)


@dataclass
class EloRating:
    """Represents an ELO rating result."""
    model_id: int
    definition_id: Optional[int]
    time_period_days: Optional[int]  # None = all-time, 7/30/90/365 = last N days
    calculation_year: Optional[int]  # None = relative period, 2024/2025 = specific year
    elo_score: float
    elo_ci_lower: float
    elo_ci_upper: float
    n_matches: int
    n_bootstraps: int
    calculation_duration_ms: int


class EloRankingService:
    """
    Service to calculate bootstrapped ELO ratings for models.
    
    This service:
    1. Loads all finalized scores (MASE values) from the database
    2. Builds a pivot matrix: rows=series_id, cols=model_id, values=MASE
    3. Runs N bootstrap iterations with shuffled series order
    4. Computes median ELO and 95% CI from bootstrap results
    """
    
    DEFAULT_K_FACTOR = 4.0
    DEFAULT_BASE_RATING = 1000.0
    DEFAULT_N_BOOTSTRAPS = 500
    
    # Time periods to calculate (relative to now)
    TIME_PERIODS = [7, 30, 90, 365]

    
    # Calendar years to calculate
    TIME_YEARS = [2024, 2025]
    
    def __init__(self, db_session: AsyncSession):
        self.session = db_session
    
    async def calculate_and_store_all_ratings(
        self,
        n_bootstraps: int = DEFAULT_N_BOOTSTRAPS,
        max_concurrent: int = 2
    ) -> Dict[str, Any]:
        """
        Calculate and store ELO ratings for:
        1. Global rankings (platform-wide) for each time period + each year
        2. Per-definition rankings for each time period + each year
        
        Note: Cumulative 'All-time' rankings (both NULL) are not calculated.
        
        Args:
            n_bootstraps: Number of bootstrap iterations per calculation
            max_concurrent: Maximum number of parallel calculations (default: 2)
        
        Returns:
            Summary dict with calculation results and timing
        """
        total_start = time.time()
        results = {
            "global": [],
            "per_definition": [],
            "total_duration_ms": 0,
            "calculated_at": datetime.now(timezone.utc)
        }
        
        logger.info(f"Starting ELO calculations with max_concurrent={max_concurrent}")
        
        # Gather all calculation tasks
        tasks = []
        
        # 1. Global ELO calculations (platform-wide, across all definitions)
        # 1a. Relative time periods (7d, 30d, 90d, 365d)
        for period in self.TIME_PERIODS:
            tasks.append(self._calculate_and_store_single(
                definition_id=None,
                time_period_days=period,
                calculation_year=None,
                n_bootstraps=n_bootstraps,
                result_type="global"
            ))
        
        # 1b. Calendar years (2024, 2025)
        for year in self.TIME_YEARS:
            tasks.append(self._calculate_and_store_single(
                definition_id=None,
                time_period_days=None,
                calculation_year=year,
                n_bootstraps=n_bootstraps,
                result_type="global"
            ))
        
        # 2. Get all definition_ids with finalized scores
        definition_ids = await self._get_definitions_with_scores()
        logger.info(f"Found {len(definition_ids)} definitions with scores")

        
        # 3. Per-definition ELO calculations
        for def_id in definition_ids:
            # 3a. Relative time periods
            for period in self.TIME_PERIODS:
                tasks.append(self._calculate_and_store_single(
                    definition_id=def_id,
                    time_period_days=period,
                    calculation_year=None,
                    n_bootstraps=n_bootstraps,
                    result_type="per_definition"
                ))
            
            # 3b. Calendar years
            for year in self.TIME_YEARS:
                tasks.append(self._calculate_and_store_single(
                    definition_id=def_id,
                    time_period_days=None,
                    calculation_year=year,
                    n_bootstraps=n_bootstraps,
                    result_type="per_definition"
                ))
        
        # Execute tasks with controlled concurrency
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def bounded_task(task):
            """Execute task with semaphore control"""
            async with semaphore:
                return await task
        
        logger.info(f"Executing {len(tasks)} ELO calculations with max {max_concurrent} concurrent")
        
        # Run all tasks with concurrency limit
        task_results = await asyncio.gather(*[bounded_task(t) for t in tasks], return_exceptions=True)
        
        # Process results and collect metrics
        for task_result in task_results:
            if isinstance(task_result, Exception):
                logger.error(f"Task failed: {task_result}", exc_info=task_result)
                continue
            
            if task_result and "result_type" in task_result:
                result_type = task_result.pop("result_type")
                results[result_type].append(task_result)
        
        total_duration = int((time.time() - total_start) * 1000)
        results["total_duration_ms"] = total_duration
        logger.info(f"ELO calculation complete. Total time: {total_duration}ms")
        
        return results


    
    async def _calculate_and_store_single(
        self,
        definition_id: Optional[int],
        time_period_days: Optional[int],
        calculation_year: Optional[int],
        n_bootstraps: int,
        result_type: str
    ) -> Dict[str, Any]:
        """
        Calculate and store ELO ratings for a single configuration.
        
        This helper method is designed to be called in parallel with controlled
        concurrency via semaphore.
        
        Args:
            definition_id: Challenge definition ID (None for global)
            time_period_days: Time period filter (None for year-based)
            calculation_year: Year filter (None for period-based)
            n_bootstraps: Number of bootstrap iterations
            result_type: "global" or "per_definition" for result categorization
            
        Returns:
            Dict with calculation results and metadata
        """
        try:
            ratings = await self.calculate_elo_ratings(
                definition_id=definition_id,
                time_period_days=time_period_days,
                calculation_year=calculation_year,
                n_bootstraps=n_bootstraps
            )
            
            if ratings:
                await self._store_ratings(ratings)
                
                result = {
                    "definition_id": definition_id,
                    "time_period_days": time_period_days,
                    "calculation_year": calculation_year,
                    "n_models": len(ratings),
                    "result_type": result_type  # For categorization
                }
                return result
            
            return None
            
        except Exception as e:
            logger.error(
                f"Failed to calculate ELO for def={definition_id}, "
                f"period={time_period_days}, year={calculation_year}: {e}",
                exc_info=True
            )
            # Return None on failure, will be filtered out
            return None
    
    async def calculate_elo_ratings(
        self,
        definition_id: Optional[int] = None,
        time_period_days: Optional[int] = None,
        calculation_year: Optional[int] = None,
        n_bootstraps: int = DEFAULT_N_BOOTSTRAPS,
        k_factor: float = DEFAULT_K_FACTOR,
        base_rating: float = DEFAULT_BASE_RATING
    ) -> List[EloRating]:
        """
        Calculate bootstrapped ELO ratings for models.
        
        Args:
            definition_id: If provided, filter to this challenge definition.
                          If None, calculate global ELO across all challenges.
            time_period_days: If provided, only include matches from the last N days.
                             If None, include all-time data.
            calculation_year: If provided, only include matches from this year.
            n_bootstraps: Number of bootstrap iterations (default 500)
            k_factor: ELO K-factor for rating updates
            base_rating: Starting ELO rating (default 1000)
            
        Returns:
            List of EloRating objects, sorted by elo_score descending
        """
        start_time = time.time()
        
        # Build scope label for logging
        if calculation_year:
            scope_label = f"def={definition_id}, year={calculation_year}"
        elif time_period_days:
            scope_label = f"def={definition_id}, period={time_period_days}d"
        else:
            scope_label = f"def={definition_id}, all-time"
        
        # Get scores matrix
        mase_matrix, match_ids, model_ids = await self._get_scores_matrix(
            definition_id=definition_id,
            time_period_days=time_period_days,
            calculation_year=calculation_year
        )
        
        if mase_matrix.size == 0 or len(model_ids) < 2:
            logger.debug(f"Not enough data for ELO ({scope_label}): "
                        f"{len(match_ids)} matches, {len(model_ids)} models")
            return []
        
        n_matches_total, n_models = mase_matrix.shape
        logger.debug(f"ELO calculation: {n_matches_total} matches, {n_models} models, "
                   f"{n_bootstraps} bootstraps ({scope_label})")
        
        # Run bootstrapped ELO in thread pool to avoid blocking event loop
        # This is CPU-intensive work that would otherwise block all other async tasks
        all_final_ratings = await asyncio.to_thread(
            self._run_all_bootstraps,
            mase_matrix=mase_matrix,
            n_bootstraps=n_bootstraps,
            k_factor=k_factor,
            base_rating=base_rating
        )
        
        # Calculate median and CI
        median_ratings = np.median(all_final_ratings, axis=0)
        ci_lower = np.percentile(all_final_ratings, 2.5, axis=0)
        ci_upper = np.percentile(all_final_ratings, 97.5, axis=0)
        
        duration_ms = int((time.time() - start_time) * 1000)
        
        # Build results
        results = []
        for i, model_id in enumerate(model_ids):
            # Count how many series this model actually participated in
            n_matches = int(np.sum(~np.isnan(mase_matrix[:, i])))
            
            results.append(EloRating(
                model_id=model_id,
                definition_id=definition_id,
                time_period_days=time_period_days,
                calculation_year=calculation_year,
                elo_score=float(median_ratings[i]),
                elo_ci_lower=float(ci_lower[i]),
                elo_ci_upper=float(ci_upper[i]),
                n_matches=n_matches,
                n_bootstraps=n_bootstraps,
                calculation_duration_ms=duration_ms
            ))
        
        # Sort by ELO descending
        results.sort(key=lambda x: x.elo_score, reverse=True)
        
        logger.debug(f"ELO done in {duration_ms}ms ({scope_label})")
        return results


    
    async def _get_scores_matrix(
        self,
        definition_id: Optional[int] = None,
        time_period_days: Optional[int] = None,
        calculation_year: Optional[int] = None
    ) -> Tuple[np.ndarray, List[Tuple[int, int]], List[int]]:
        """
        Build pivot matrix: rows=(round_id, series_id) matches, cols=model_id, values=MASE.
        
        Args:
            definition_id: Filter to this definition (None = all)
            time_period_days: Filter to rounds ending in last N days (None = all-time)
            calculation_year: Filter to rounds ending in a specific year
        
        Returns:
            tuple: (mase_matrix, match_ids, model_ids)
        """
        base_query = """
            SELECT fs.round_id, fs.series_id, fs.model_id, fs.mase
            FROM forecasts.scores fs
            JOIN challenges.rounds cr ON fs.round_id = cr.id
            WHERE fs.final_evaluation = TRUE
              AND fs.mase IS NOT NULL
              AND fs.mase != 'NaN'
              AND fs.mase != 'Infinity'
              AND fs.mase != '-Infinity'
        """
        
        params = {}
        
        if definition_id is not None:
            base_query += " AND cr.definition_id = :definition_id"
            params["definition_id"] = definition_id
        
        if time_period_days is not None:
            base_query += " AND cr.end_time >= NOW() - INTERVAL ':days days'"
            base_query = base_query.replace(":days", str(int(time_period_days)))
            
        if calculation_year is not None:
            base_query += " AND EXTRACT(YEAR FROM cr.end_time) = :year"
            params["year"] = calculation_year
        
        base_query += " ORDER BY fs.round_id, fs.series_id, fs.model_id"
        
        result = await self.session.execute(text(base_query), params)
        rows = result.fetchall()

        
        if not rows:
            return np.array([]), [], []
        
        # Build pivot matrix in thread pool to avoid blocking event loop
        # This can be CPU-intensive with large datasets
        return await asyncio.to_thread(self._build_matrix_from_rows, rows)
    
    def _build_matrix_from_rows(
        self,
        rows: List[Tuple]
    ) -> Tuple[np.ndarray, List[Tuple[int, int]], List[int]]:
        """
        Build pivot matrix from query rows. Runs in thread pool.
        
        Args:
            rows: List of (round_id, series_id, model_id, mase) tuples
            
        Returns:
            tuple: (mase_matrix, match_ids, model_ids)
        """
        # Build pivot matrix
        # Key: (round_id, series_id) = one match
        match_set = set()
        model_set = set()
        data_dict = {}
        
        for row in rows:
            round_id, series_id, model_id, mase = row
            match_key = (round_id, series_id)
            match_set.add(match_key)
            model_set.add(model_id)
            data_dict[(match_key, model_id)] = mase
        
        match_ids = sorted(match_set)
        model_ids = sorted(model_set)
        
        match_idx = {m: i for i, m in enumerate(match_ids)}
        model_idx = {m: i for i, m in enumerate(model_ids)}
        
        # Create matrix with NaN for missing values
        matrix = np.full((len(match_ids), len(model_ids)), np.nan)
        
        for (match_key, model_id), mase in data_dict.items():
            i = match_idx[match_key]
            j = model_idx[model_id]
            matrix[i, j] = mase
        
        return matrix, match_ids, model_ids


    
    def _run_all_bootstraps(
        self,
        mase_matrix: np.ndarray,
        n_bootstraps: int,
        k_factor: float,
        base_rating: float
    ) -> np.ndarray:
        """
        Run all bootstrap iterations in a thread-safe manner.
        
        This method is designed to run in a thread pool via asyncio.to_thread()
        to avoid blocking the async event loop with CPU-intensive calculations.
        
        Args:
            mase_matrix: (n_series, n_models) matrix of MASE values
            n_bootstraps: Number of bootstrap iterations to run
            k_factor: ELO K-factor
            base_rating: Starting rating for all models
            
        Returns:
            Array of shape (n_bootstraps, n_models) with final ratings
        """
        n_models = mase_matrix.shape[1]
        all_final_ratings = np.zeros((n_bootstraps, n_models))
        
        for b in range(n_bootstraps):
            all_final_ratings[b] = self._run_single_bootstrap(
                mase_matrix=mase_matrix,
                k_factor=k_factor,
                base_rating=base_rating
            )
        
        return all_final_ratings
    
    def _run_single_bootstrap(
        self,
        mase_matrix: np.ndarray,
        k_factor: float,
        base_rating: float
    ) -> np.ndarray:
        """
        Run single ELO "season" with shuffled series order.
        
        Uses NumPy vectorization for efficient all-vs-all comparison per series.
        
        Args:
            mase_matrix: (n_series, n_models) matrix of MASE values (NaN for missing)
            k_factor: ELO K-factor
            base_rating: Starting rating for all models
            
        Returns:
            Final ratings array of shape (n_models,)
        """
        n_series, n_models = mase_matrix.shape
        ratings = np.full(n_models, base_rating)
        
        # Shuffle series order
        series_order = np.random.permutation(n_series)
        
        for series_idx in series_order:
            mase_values = mase_matrix[series_idx]
            
            # Find models that participated (non-NaN MASE)
            valid_mask = ~np.isnan(mase_values)
            valid_indices = np.where(valid_mask)[0]
            
            if len(valid_indices) < 2:
                continue  # Need at least 2 models for a match
            
            # Get current ratings and MASE for valid models
            current_ratings = ratings[valid_indices]
            current_mase = mase_values[valid_indices]
            n_valid = len(valid_indices)
            
            # Compute rating changes using all-vs-all comparison
            rating_changes = np.zeros(n_valid)
            
            for i in range(n_valid):
                actual_score_sum = 0.0
                expected_score_sum = 0.0
                
                for j in range(n_valid):
                    if i == j:
                        continue
                    
                    # Outcome based on MASE (lower is better)
                    if current_mase[i] < current_mase[j]:
                        outcome = 1.0  # Win
                    elif current_mase[i] == current_mase[j]:
                        outcome = 0.5  # Draw
                    else:
                        outcome = 0.0  # Loss
                    
                    # Expected score using ELO formula
                    ra = current_ratings[i]
                    rb = current_ratings[j]
                    expected = 1.0 / (1.0 + 10.0 ** ((rb - ra) / 400.0))
                    
                    actual_score_sum += outcome
                    expected_score_sum += expected
                
                # K-factor normalized by number of opponents
                rating_changes[i] = k_factor * (actual_score_sum - expected_score_sum)
            
            # Apply updates
            ratings[valid_indices] += rating_changes
        
        return ratings
    
    async def _get_definitions_with_scores(self) -> List[int]:
        """Get all definition_ids that have finalized scores."""
        query = text("""
            SELECT DISTINCT cr.definition_id
            FROM forecasts.scores fs
            JOIN challenges.rounds cr ON fs.round_id = cr.id
            WHERE fs.final_evaluation = TRUE
              AND fs.mase IS NOT NULL
              AND cr.definition_id IS NOT NULL
            ORDER BY cr.definition_id
        """)
        result = await self.session.execute(query)
        return [row[0] for row in result.fetchall()]
    
    async def _store_ratings(self, ratings: List[EloRating]) -> int:
        """
        Store ELO ratings using INSERT ... ON CONFLICT DO UPDATE.
        
        Returns:
            Number of rows affected
        """
        if not ratings:
            return 0
        
        # Use raw SQL for UPSERT with COALESCE for NULL handling
        # Unique key: (model_id, definition_id, time_period_days, calculation_year)
        query = text("""
            INSERT INTO forecasts.elo_ratings 
                (model_id, definition_id, time_period_days, calculation_year, 
                 elo_score, elo_ci_lower, elo_ci_upper, 
                 n_matches, n_bootstraps, calculation_duration_ms, calculated_at)
            VALUES 
                (:model_id, :definition_id, :time_period_days, :calculation_year,
                 :elo_score, :elo_ci_lower, :elo_ci_upper,
                 :n_matches, :n_bootstraps, :calculation_duration_ms, NOW())
            ON CONFLICT (model_id, COALESCE(definition_id, -1), COALESCE(time_period_days, 0), COALESCE(calculation_year, 0))
            DO UPDATE SET
                elo_score = EXCLUDED.elo_score,
                elo_ci_lower = EXCLUDED.elo_ci_lower,
                elo_ci_upper = EXCLUDED.elo_ci_upper,
                n_matches = EXCLUDED.n_matches,
                n_bootstraps = EXCLUDED.n_bootstraps,
                calculation_duration_ms = EXCLUDED.calculation_duration_ms,
                calculated_at = NOW()
        """)
        
        # Build all parameter dicts for batch execution
        params_list = [
            {
                "model_id": rating.model_id,
                "definition_id": rating.definition_id,
                "time_period_days": rating.time_period_days,
                "calculation_year": rating.calculation_year,
                "elo_score": rating.elo_score,
                "elo_ci_lower": rating.elo_ci_lower,
                "elo_ci_upper": rating.elo_ci_upper,
                "n_matches": rating.n_matches,
                "n_bootstraps": rating.n_bootstraps,
                "calculation_duration_ms": rating.calculation_duration_ms
            }
            for rating in ratings
        ]
        
        # Execute as batch to reduce database round-trips
        for params in params_list:
            await self.session.execute(query, params)
        
        await self.session.commit()
        return len(ratings)


    
    async def has_calculated_today(self) -> bool:
        """
        Check if ELO ratings have already been calculated today.
        
        Returns:
            True if global ELO was calculated today, False otherwise
        """
        query = text("""
            SELECT 1 FROM forecasts.elo_ratings
            WHERE definition_id IS NULL
              AND calculated_at::date = CURRENT_DATE
            LIMIT 1
        """)
        result = await self.session.execute(query)
        return result.fetchone() is not None
    
    async def get_leaderboard(
        self,
        definition_id: Optional[int] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get ELO leaderboard from stored ratings.
        
        Args:
            definition_id: None for global, or specific definition_id
            limit: Maximum number of results
            
        Returns:
            List of leaderboard entries with model info
        """
        if definition_id is None:
            query = text("""
                SELECT * FROM forecasts.v_elo_leaderboard
                WHERE definition_id IS NULL
                ORDER BY elo_score DESC
                LIMIT :limit
            """)
            result = await self.session.execute(query, {"limit": limit})
        else:
            query = text("""
                SELECT * FROM forecasts.v_elo_leaderboard
                WHERE definition_id = :definition_id
                ORDER BY elo_score DESC
                LIMIT :limit
            """)
            result = await self.session.execute(query, {
                "definition_id": definition_id,
                "limit": limit
            })
        
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]

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
    
    # Time periods to calculate: None = all-time, then 7, 30, 90, 365 days
    TIME_PERIODS = [None, 7, 30, 90, 365]
    
    def __init__(self, db_session: AsyncSession):
        self.session = db_session
    
    async def calculate_and_store_all_ratings(
        self,
        n_bootstraps: int = DEFAULT_N_BOOTSTRAPS
    ) -> Dict[str, Any]:
        """
        Calculate and store ELO ratings for:
        1. Global rankings (all-time + each time period)
        2. Per-definition rankings (all-time + each time period)
        
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
        
        # 1. Calculate global ELO for all time periods
        logger.info("Calculating global ELO ratings for all time periods...")
        for period in self.TIME_PERIODS:
            period_label = f"{period}d" if period else "all-time"
            try:
                ratings = await self.calculate_elo_ratings(
                    definition_id=None,
                    time_period_days=period,
                    n_bootstraps=n_bootstraps
                )
                if ratings:
                    await self._store_ratings(ratings)
                    results["global"].append({
                        "time_period_days": period,
                        "n_models": len(ratings),
                        "duration_ms": ratings[0].calculation_duration_ms if ratings else 0
                    })
                    logger.info(f"Global ELO ({period_label}): {len(ratings)} models rated")
            except Exception as e:
                logger.exception(f"Failed global ELO for {period_label}: {e}")
        
        # 2. Get all definition_ids with finalized scores
        definition_ids = await self._get_definitions_with_scores()
        logger.info(f"Found {len(definition_ids)} definitions with scores")
        
        # 3. Calculate per-definition ELO for all time periods
        for def_id in definition_ids:
            for period in self.TIME_PERIODS:
                period_label = f"{period}d" if period else "all-time"
                try:
                    ratings = await self.calculate_elo_ratings(
                        definition_id=def_id,
                        time_period_days=period,
                        n_bootstraps=n_bootstraps
                    )
                    if ratings:
                        await self._store_ratings(ratings)
                        results["per_definition"].append({
                            "definition_id": def_id,
                            "time_period_days": period,
                            "n_models": len(ratings),
                            "duration_ms": ratings[0].calculation_duration_ms if ratings else 0
                        })
                        logger.debug(f"Definition {def_id} ELO ({period_label}): {len(ratings)} models")
                except Exception as e:
                    logger.exception(f"Failed ELO for def {def_id} ({period_label}): {e}")
        
        total_duration = int((time.time() - total_start) * 1000)
        results["total_duration_ms"] = total_duration
        logger.info(f"ELO calculation complete. Total time: {total_duration}ms")
        
        return results

    
    async def calculate_elo_ratings(
        self,
        definition_id: Optional[int] = None,
        time_period_days: Optional[int] = None,
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
            n_bootstraps: Number of bootstrap iterations (default 500)
            k_factor: ELO K-factor for rating updates
            base_rating: Starting ELO rating (default 1000)
            
        Returns:
            List of EloRating objects, sorted by elo_score descending
        """
        start_time = time.time()
        scope_label = f"def={definition_id}, period={time_period_days}d" if time_period_days else f"def={definition_id}, all-time"
        
        # Get scores matrix - match_ids are (round_id, series_id) tuples
        mase_matrix, match_ids, model_ids = await self._get_scores_matrix(
            definition_id=definition_id,
            time_period_days=time_period_days
        )
        
        if mase_matrix.size == 0 or len(model_ids) < 2:
            logger.debug(f"Not enough data for ELO ({scope_label}): "
                        f"{len(match_ids)} matches, {len(model_ids)} models")
            return []
        
        n_matches_total, n_models = mase_matrix.shape
        logger.debug(f"ELO calculation: {n_matches_total} matches, {n_models} models, "
                   f"{n_bootstraps} bootstraps ({scope_label})")
        
        # Run bootstrapped ELO
        all_final_ratings = np.zeros((n_bootstraps, n_models))

        
        for b in range(n_bootstraps):
            final_ratings = self._run_single_bootstrap(
                mase_matrix=mase_matrix,
                k_factor=k_factor,
                base_rating=base_rating
            )
            all_final_ratings[b] = final_ratings
        
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
        time_period_days: Optional[int] = None
    ) -> Tuple[np.ndarray, List[Tuple[int, int]], List[int]]:
        """
        Build pivot matrix: rows=(round_id, series_id) matches, cols=model_id, values=MASE.
        
        Each unique combination of (round_id, series_id) is a "match" where models compete.
        
        Args:
            definition_id: Filter to this definition (None = all)
            time_period_days: Filter to rounds ending in last N days (None = all-time)
        
        Returns:
            tuple: (mase_matrix, match_ids, model_ids)
                   where match_ids are (round_id, series_id) tuples
        """
        # Build dynamic query with optional filters
        # Include round_id to properly identify each match
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
        
        # Add definition filter
        if definition_id is not None:
            base_query += " AND cr.definition_id = :definition_id"
            params["definition_id"] = definition_id
        
        # Add time period filter
        if time_period_days is not None:
            base_query += " AND cr.end_time >= NOW() - INTERVAL ':days days'"
            # Note: interval syntax requires direct substitution for safety
            base_query = base_query.replace(":days", str(int(time_period_days)))
        
        base_query += " ORDER BY fs.round_id, fs.series_id, fs.model_id"
        
        result = await self.session.execute(text(base_query), params)
        rows = result.fetchall()
        
        if not rows:
            return np.array([]), [], []
        
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
        # The unique index is on (model_id, COALESCE(definition_id, -1), COALESCE(time_period_days, 0))
        query = text("""
            INSERT INTO forecasts.elo_ratings 
                (model_id, definition_id, time_period_days, elo_score, elo_ci_lower, elo_ci_upper, 
                 n_matches, n_bootstraps, calculation_duration_ms, calculated_at)
            VALUES 
                (:model_id, :definition_id, :time_period_days, :elo_score, :elo_ci_lower, :elo_ci_upper,
                 :n_matches, :n_bootstraps, :calculation_duration_ms, NOW())
            ON CONFLICT (model_id, COALESCE(definition_id, -1), COALESCE(time_period_days, 0))
            DO UPDATE SET
                elo_score = EXCLUDED.elo_score,
                elo_ci_lower = EXCLUDED.elo_ci_lower,
                elo_ci_upper = EXCLUDED.elo_ci_upper,
                n_matches = EXCLUDED.n_matches,
                n_bootstraps = EXCLUDED.n_bootstraps,
                calculation_duration_ms = EXCLUDED.calculation_duration_ms,
                calculated_at = NOW()
        """)
        
        for rating in ratings:
            await self.session.execute(query, {
                "model_id": rating.model_id,
                "definition_id": rating.definition_id,
                "time_period_days": rating.time_period_days,
                "elo_score": rating.elo_score,
                "elo_ci_lower": rating.elo_ci_lower,
                "elo_ci_upper": rating.elo_ci_upper,
                "n_matches": rating.n_matches,
                "n_bootstraps": rating.n_bootstraps,
                "calculation_duration_ms": rating.calculation_duration_ms
            })
        
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

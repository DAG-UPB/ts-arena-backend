import asyncio
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone, date
from dataclasses import dataclass
import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

logger = logging.getLogger(__name__)


@dataclass
class EloRating:
    """Represents an ELO rating result."""
    model_id: int
    scope_type: str  # 'global', 'definition', 'frequency_horizon'
    scope_id: Optional[str]  # None for global, definition_id, or "frequency::horizon"
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
    2. Builds a pivot matrix: rows=round_id matches, cols=model_id, values=AVG(MASE)
    3. Runs N bootstrap iterations with shuffled round order
    4. Computes median ELO and 95% CI from bootstrap results
    5. Stores daily snapshots in forecasts.daily_rankings
    
    Supports three scope types:
    - global: platform-wide ranking across all challenges
    - definition: per challenge definition ranking
    - frequency_horizon: grouped by frequency+horizon combination
    """
    
    DEFAULT_K_FACTOR = 4.0
    DEFAULT_BASE_RATING = 1000.0
    DEFAULT_N_BOOTSTRAPS = 500
    
    def __init__(self, db_session: AsyncSession):
        self.session = db_session
    
    async def calculate_and_store_all_ratings(
        self,
        n_bootstraps: int = DEFAULT_N_BOOTSTRAPS,
        calculation_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Calculate and store ELO ratings for all scopes:
        1. Global rankings (platform-wide)
        2. Per-definition rankings
        3. Per-frequency+horizon rankings (dynamically from challenges.definitions)
        
        Uses FULL historical data - no time-window truncation.
        
        Args:
            n_bootstraps: Number of bootstrap iterations per calculation
            calculation_date: Date for the snapshot (default: today)
        
        Returns:
            Summary dict with calculation results and timing
        """
        total_start = time.time()
        calc_date = calculation_date or date.today()
        
        results = {
            "global": [],
            "per_definition": [],
            "per_frequency_horizon": [],
            "total_duration_ms": 0,
            "calculation_date": calc_date,
            "calculated_at": datetime.now(timezone.utc)
        }
        
        calculations = []
        
        # 1. Global ELO (platform-wide, all data)
        calculations.append({
            "scope_type": "global",
            "scope_id": None,
            "definition_id": None,
            "frequency": None,
            "horizon": None,
            "label": "Global"
        })
        
        # 2. Get all definition_ids with finalized scores
        definition_ids = await self._get_definitions_with_scores()
        logger.info(f"Found {len(definition_ids)} definitions with scores")
        
        for def_id in definition_ids:
            calculations.append({
                "scope_type": "definition",
                "scope_id": str(def_id),
                "definition_id": def_id,
                "frequency": None,
                "horizon": None,
                "label": f"Definition {def_id}"
            })
        
        # 3. Get unique frequency+horizon combinations from challenges.definitions
        freq_horizon_groups = await self._get_frequency_horizon_groups()
        logger.info(f"Found {len(freq_horizon_groups)} frequency+horizon groups")
        
        for frequency, horizon in freq_horizon_groups:
            scope_id = f"{frequency}::{horizon}"
            calculations.append({
                "scope_type": "frequency_horizon",
                "scope_id": scope_id,
                "definition_id": None,
                "frequency": frequency,
                "horizon": horizon,
                "label": f"FreqHorizon {scope_id}"
            })
        
        total_calculations = len(calculations)
        logger.info(f"Running {total_calculations} ELO calculations")
        
        # Execute calculations ONE AT A TIME
        completed = 0
        failed = 0
        
        for calc in calculations:
            try:
                label = calc.pop("label")
                calc_start = time.time()
                
                result = await self._calculate_and_store_single(
                    **calc,
                    n_bootstraps=n_bootstraps,
                    calculation_date=calc_date
                )
                
                calc_duration = int((time.time() - calc_start) * 1000)
                completed += 1
                
                if result:
                    result_type = result.get("scope_type", "per_definition")
                    if result_type == "global":
                        results["global"].append(result)
                    elif result_type == "definition":
                        results["per_definition"].append(result)
                    else:
                        results["per_frequency_horizon"].append(result)
                    logger.info(f"[{completed}/{total_calculations}] ✓ {label} - {calc_duration}ms")
                else:
                    logger.info(f"[{completed}/{total_calculations}] ○ {label} - no data ({calc_duration}ms)")
                    
            except Exception as e:
                failed += 1
                completed += 1
                logger.error(f"[{completed}/{total_calculations}] ✗ {calc.get('label', 'unknown')} failed: {e}")
        
        total_duration = int((time.time() - total_start) * 1000)
        results["total_duration_ms"] = total_duration
        
        logger.info(
            f"ELO calculation complete. "
            f"Total: {completed}, Failed: {failed}, "
            f"Duration: {total_duration}ms ({total_duration/1000:.1f}s)"
        )
        
        return results

    
    async def _calculate_and_store_single(
        self,
        scope_type: str,
        scope_id: Optional[str],
        definition_id: Optional[int],
        frequency: Optional[str],
        horizon: Optional[str],
        n_bootstraps: int,
        calculation_date: date
    ) -> Optional[Dict[str, Any]]:
        """
        Calculate and store ELO ratings for a single scope configuration.
        
        Args:
            scope_type: 'global', 'definition', or 'frequency_horizon'
            scope_id: Identifier for the scope (None for global)
            definition_id: Challenge definition ID (for definition scope)
            frequency: Frequency interval (for frequency_horizon scope)
            horizon: Horizon interval (for frequency_horizon scope)
            n_bootstraps: Number of bootstrap iterations
            calculation_date: Date for the snapshot
            
        Returns:
            Dict with calculation results and metadata
        """
        try:
            ratings = await self.calculate_elo_ratings(
                definition_id=definition_id,
                frequency=frequency,
                horizon=horizon,
                n_bootstraps=n_bootstraps
            )
            
            if ratings:
                await self._store_ratings(
                    ratings=ratings,
                    scope_type=scope_type,
                    scope_id=scope_id,
                    calculation_date=calculation_date
                )
                
                return {
                    "scope_type": scope_type,
                    "scope_id": scope_id,
                    "n_models": len(ratings),
                }
            
            return None
            
        except Exception as e:
            logger.error(
                f"Failed to calculate ELO for scope={scope_type}, "
                f"scope_id={scope_id}: {e}",
                exc_info=True
            )
            return None
    
    async def calculate_elo_ratings(
        self,
        definition_id: Optional[int] = None,
        frequency: Optional[str] = None,
        horizon: Optional[str] = None,
        n_bootstraps: int = DEFAULT_N_BOOTSTRAPS,
        k_factor: float = DEFAULT_K_FACTOR,
        base_rating: float = DEFAULT_BASE_RATING
    ) -> List[EloRating]:
        """
        Calculate bootstrapped ELO ratings for models.
        
        Uses FULL historical data - no time-window truncation.
        
        Args:
            definition_id: If provided, filter to this challenge definition.
                          If None, calculate across relevant challenges.
            frequency: If provided with horizon, filter by frequency+horizon.
            horizon: If provided with frequency, filter by frequency+horizon.
            n_bootstraps: Number of bootstrap iterations (default 500)
            k_factor: ELO K-factor for rating updates
            base_rating: Starting ELO rating (default 1000)
            
        Returns:
            List of EloRating objects, sorted by elo_score descending
        """
        start_time = time.time()
        
        # Build scope label for logging
        if definition_id:
            scope_label = f"definition={definition_id}"
            scope_type = "definition"
            scope_id = str(definition_id)
        elif frequency and horizon:
            scope_label = f"freq_horizon={frequency}::{horizon}"
            scope_type = "frequency_horizon"
            scope_id = f"{frequency}::{horizon}"
        else:
            scope_label = "global"
            scope_type = "global"
            scope_id = None
        
        # Get scores matrix
        mase_matrix, match_ids, model_ids = await self._get_scores_matrix(
            definition_id=definition_id,
            frequency=frequency,
            horizon=horizon
        )
        
        if mase_matrix.size == 0 or len(model_ids) < 2:
            logger.debug(f"Not enough data for ELO ({scope_label}): "
                        f"{len(match_ids)} matches, {len(model_ids)} models")
            return []
        
        n_matches_total, n_models = mase_matrix.shape
        logger.debug(f"ELO calculation: {n_matches_total} matches, {n_models} models, "
                   f"{n_bootstraps} bootstraps ({scope_label})")
        
        # Run bootstrapped ELO in thread pool to avoid blocking event loop
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
            # Count how many rounds this model actually participated in
            n_matches = int(np.sum(~np.isnan(mase_matrix[:, i])))
            
            results.append(EloRating(
                model_id=model_id,
                scope_type=scope_type,
                scope_id=scope_id,
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
        frequency: Optional[str] = None,
        horizon: Optional[str] = None
    ) -> Tuple[np.ndarray, List[int], List[int]]:
        """
        Build pivot matrix: rows=round_id matches, cols=model_id, values=AVG(MASE).
        
        Aggregates MASE values per round (averaging across all series in a round)
        to reduce the number of pairwise comparisons.
        
        Uses FULL historical data - no time-window truncation.
        
        Args:
            definition_id: Filter to this definition (None = use other filters)
            frequency: Filter by frequency interval (requires horizon too)
            horizon: Filter by horizon interval (requires frequency too)
        
        Returns:
            tuple: (mase_matrix, round_ids, model_ids)
        """
        # Aggregate MASE per round and model (average across all series in a round)
        base_query = """
            SELECT fs.round_id, fs.model_id, AVG(fs.mase) as avg_mase
            FROM forecasts.scores fs
            JOIN challenges.rounds cr ON fs.round_id = cr.id
            WHERE fs.final_evaluation = TRUE
              AND fs.mase IS NOT NULL
              AND fs.mase != 'NaN'
              AND fs.mase != 'Infinity'
              AND fs.mase != '-Infinity'
              -- Exclude series marked as excluded in definition_series_scd2
              AND NOT EXISTS (
                  SELECT 1 FROM challenges.definition_series_scd2 ds
                  WHERE ds.definition_id = cr.definition_id 
                    AND ds.series_id = fs.series_id
                    AND ds.is_excluded = TRUE
              )
        """
        
        params = {}
        
        if definition_id is not None:
            base_query += " AND cr.definition_id = :definition_id"
            params["definition_id"] = definition_id
        elif frequency is not None and horizon is not None:
            # Filter by frequency+horizon via challenges.definitions
            base_query += """
                AND cr.definition_id IN (
                    SELECT id FROM challenges.definitions 
                    WHERE frequency = CAST(:frequency AS interval) 
                      AND horizon = CAST(:horizon AS interval)
                )
            """
            params["frequency"] = frequency
            params["horizon"] = horizon
        
        base_query += " GROUP BY fs.round_id, fs.model_id"
        base_query += " ORDER BY fs.round_id, fs.model_id"
        
        result = await self.session.execute(text(base_query), params)
        rows = result.fetchall()

        
        if not rows:
            return np.array([]), [], []
        
        # Build pivot matrix in thread pool to avoid blocking event loop
        return await asyncio.to_thread(self._build_matrix_from_rows, rows)
    
    def _build_matrix_from_rows(
        self,
        rows: List[Tuple]
    ) -> Tuple[np.ndarray, List[int], List[int]]:
        """
        Build pivot matrix from query rows. Runs in thread pool.
        
        Args:
            rows: List of (round_id, model_id, avg_mase) tuples
            
        Returns:
            tuple: (mase_matrix, round_ids, model_ids)
        """
        round_set = set()
        model_set = set()
        data_dict = {}
        
        for row in rows:
            round_id, model_id, avg_mase = row
            round_set.add(round_id)
            model_set.add(model_id)
            data_dict[(round_id, model_id)] = avg_mase
        
        round_ids = sorted(round_set)
        model_ids = sorted(model_set)
        
        round_idx = {r: i for i, r in enumerate(round_ids)}
        model_idx = {m: i for i, m in enumerate(model_ids)}
        
        # Create matrix with NaN for missing values
        matrix = np.full((len(round_ids), len(model_ids)), np.nan)
        
        for (round_id, model_id), avg_mase in data_dict.items():
            i = round_idx[round_id]
            j = model_idx[model_id]
            matrix[i, j] = avg_mase
        
        return matrix, round_ids, model_ids


    
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
        Run single ELO "season" with shuffled round order.
        
        Uses NumPy vectorization for efficient all-vs-all comparison per round.
        """
        n_rounds, n_models = mase_matrix.shape
        ratings = np.full(n_models, base_rating)
        
        # Shuffle round order
        round_order = np.random.permutation(n_rounds)
        
        for round_idx in round_order:
            mase_values = mase_matrix[round_idx]
            
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
    
    async def _get_frequency_horizon_groups(self) -> List[Tuple[str, str]]:
        """
        Get unique frequency+horizon combinations from challenges.definitions
        that have finalized scores.
        """
        query = text("""
            SELECT DISTINCT cd.frequency::text AS freq, cd.horizon::text AS hor
            FROM challenges.definitions cd
            JOIN challenges.rounds cr ON cr.definition_id = cd.id
            JOIN forecasts.scores fs ON fs.round_id = cr.id
            WHERE fs.final_evaluation = TRUE
              AND fs.mase IS NOT NULL
              AND cd.frequency IS NOT NULL
              AND cd.horizon IS NOT NULL
            ORDER BY freq, hor
        """)
        result = await self.session.execute(query)
        return [(row[0], row[1]) for row in result.fetchall()]
    
    async def _store_ratings(
        self,
        ratings: List[EloRating],
        scope_type: str,
        scope_id: Optional[str],
        calculation_date: date
    ) -> int:
        """
        Store ELO ratings using INSERT ... ON CONFLICT DO UPDATE.
        
        Returns:
            Number of rows affected
        """
        if not ratings:
            return 0
        
        # Calculate rank positions
        sorted_ratings = sorted(ratings, key=lambda x: x.elo_score, reverse=True)
        rank_map = {r.model_id: idx + 1 for idx, r in enumerate(sorted_ratings)}
        
        query = text("""
            INSERT INTO forecasts.daily_rankings 
                (calculation_date, model_id, scope_type, scope_id,
                 elo_rating_median, elo_ci_lower, elo_ci_upper,
                 matches_played, rank_position, n_bootstraps, calculation_duration_ms, calculated_at)
            VALUES 
                (:calculation_date, :model_id, :scope_type, :scope_id,
                 :elo_rating_median, :elo_ci_lower, :elo_ci_upper,
                 :matches_played, :rank_position, :n_bootstraps, :calculation_duration_ms, NOW())
            ON CONFLICT (calculation_date, model_id, scope_type, COALESCE(scope_id, ''))
            DO UPDATE SET
                elo_rating_median = EXCLUDED.elo_rating_median,
                elo_ci_lower = EXCLUDED.elo_ci_lower,
                elo_ci_upper = EXCLUDED.elo_ci_upper,
                matches_played = EXCLUDED.matches_played,
                rank_position = EXCLUDED.rank_position,
                n_bootstraps = EXCLUDED.n_bootstraps,
                calculation_duration_ms = EXCLUDED.calculation_duration_ms,
                calculated_at = NOW()
        """)
        
        for rating in ratings:
            params = {
                "calculation_date": calculation_date,
                "model_id": rating.model_id,
                "scope_type": scope_type,
                "scope_id": scope_id,
                "elo_rating_median": rating.elo_score,
                "elo_ci_lower": rating.elo_ci_lower,
                "elo_ci_upper": rating.elo_ci_upper,
                "matches_played": rating.n_matches,
                "rank_position": rank_map.get(rating.model_id),
                "n_bootstraps": rating.n_bootstraps,
                "calculation_duration_ms": rating.calculation_duration_ms
            }
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
            SELECT 1 FROM forecasts.daily_rankings
            WHERE scope_type = 'global'
              AND calculation_date = CURRENT_DATE
            LIMIT 1
        """)
        result = await self.session.execute(query)
        return result.fetchone() is not None
    
    async def get_leaderboard(
        self,
        scope_type: str = "global",
        scope_id: Optional[str] = None,
        calculation_date: Optional[date] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get ELO leaderboard from stored daily rankings.
        
        Args:
            scope_type: 'global', 'definition', or 'frequency_horizon'
            scope_id: Identifier for the scope (None for global)
            calculation_date: Date to fetch (default: most recent)
            limit: Maximum number of results
            
        Returns:
            List of leaderboard entries with model info
        """
        if calculation_date:
            date_filter = "dr.calculation_date = :calc_date"
            params = {"calc_date": calculation_date, "scope_type": scope_type, "limit": limit}
        else:
            # Get most recent date for this scope
            date_filter = """dr.calculation_date = (
                SELECT MAX(calculation_date) FROM forecasts.daily_rankings 
                WHERE scope_type = :scope_type AND COALESCE(scope_id, '') = COALESCE(:scope_id, '')
            )"""
            params = {"scope_type": scope_type, "scope_id": scope_id, "limit": limit}
        
        if scope_id is None:
            scope_filter = "dr.scope_id IS NULL"
        else:
            scope_filter = "dr.scope_id = :scope_id"
            params["scope_id"] = scope_id
        
        query = text(f"""
            SELECT * FROM forecasts.v_daily_rankings_leaderboard dr
            WHERE dr.scope_type = :scope_type
              AND {scope_filter}
              AND {date_filter}
            ORDER BY dr.elo_rating_median DESC
            LIMIT :limit
        """)
        
        result = await self.session.execute(query, params)
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]

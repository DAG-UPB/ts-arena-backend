import asyncio
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone, date, timedelta
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

    # Overall-ranking (global scope) full-participation eligibility.
    # A model is listed in the GLOBAL ranking only if, for EVERY challenge in the universe,
    # it has (a) seen the challenge (>=1 round since it joined the platform) and
    # (b) covered at least this fraction of that challenge's rounds available since it joined.
    # Non-global scopes (definition / frequency_horizon) are unaffected. See issue ts-arena-1.
    DEFAULT_PARTICIPATION_TAU = 0.5

    # Metric -> forecasts.scores column driving the ranking. Both are lower-is-better, so the
    # bootstrap outcome logic is unchanged; only the source column differs.
    _METRIC_COLUMN: Dict[str, str] = {"mase": "mase", "sql": "sql_score"}
    SUPPORTED_METRICS: Tuple[str, ...] = ("mase", "sql")

    def __init__(self, db_session: AsyncSession):
        self.session = db_session

    def _metric_column(self, metric: str) -> str:
        """Resolve a metric name to its whitelisted scores column (guards SQL injection)."""
        try:
            return self._METRIC_COLUMN[metric]
        except KeyError:
            raise ValueError(f"Unsupported ranking metric '{metric}'; expected one of {self.SUPPORTED_METRICS}")

    async def calculate_and_store_all_ratings(
        self,
        n_bootstraps: int = DEFAULT_N_BOOTSTRAPS,
        calculation_date: Optional[date] = None,
        metric: str = "mase"
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
        definition_ids = await self._get_definitions_with_scores(metric=metric)
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
        freq_horizon_groups = await self._get_frequency_horizon_groups(metric=metric)
        logger.info(f"Found {len(freq_horizon_groups)} frequency+horizon groups")
        
        for scope_id, frequency, horizon in freq_horizon_groups:
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
                    calculation_date=calc_date,
                    score_cutoff_date=calc_date,
                    metric=metric
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
        frequency: Optional[timedelta],
        horizon: Optional[timedelta],
        n_bootstraps: int,
        calculation_date: date,
        score_cutoff_date: Optional[date] = None,
        metric: str = "mase"
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
                n_bootstraps=n_bootstraps,
                score_cutoff_date=score_cutoff_date,
                metric=metric
            )

            if ratings:
                await self._store_ratings(
                    ratings=ratings,
                    scope_type=scope_type,
                    scope_id=scope_id,
                    calculation_date=calculation_date,
                    metric=metric
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
        frequency: Optional[timedelta] = None,
        horizon: Optional[timedelta] = None,
        n_bootstraps: int = DEFAULT_N_BOOTSTRAPS,
        k_factor: float = DEFAULT_K_FACTOR,
        base_rating: float = DEFAULT_BASE_RATING,
        score_cutoff_date: Optional[date] = None,
        metric: str = "mase"
    ) -> List[EloRating]:
        """
        Calculate bootstrapped ELO ratings for models.

        Uses full historical data by default. If score_cutoff_date is set,
        only rounds with registration_start::date <= cutoff are considered —
        used for reconstructing historical month-end snapshots.

        Args:
            definition_id: If provided, filter to this challenge definition.
                          If None, calculate across relevant challenges.
            frequency: If provided with horizon, filter by frequency+horizon.
            horizon: If provided with frequency, filter by frequency+horizon.
            n_bootstraps: Number of bootstrap iterations (default 500)
            k_factor: ELO K-factor for rating updates
            base_rating: Starting ELO rating (default 1000)
            score_cutoff_date: If set, only include rounds that started on
                              or before this date.

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
        
        # Get scores matrix (values are the selected metric; both are lower-is-better)
        mase_matrix, match_ids, model_ids = await self._get_scores_matrix(
            definition_id=definition_id,
            frequency=frequency,
            horizon=horizon,
            score_cutoff_date=score_cutoff_date,
            metric=metric
        )

        # GLOBAL scope only: restrict to models that fully participate across all challenges
        # (see ts-arena-1). Definition / frequency_horizon scopes keep every model, so a model
        # excluded from the overall ranking still appears in its challenge-specific views.
        if scope_type == "global" and mase_matrix.size > 0:
            eligible = await self._get_eligible_global_model_ids(
                score_cutoff_date=score_cutoff_date, metric=metric
            )
            excluded = [m for m in model_ids if m not in eligible]
            if excluded:
                keep = [i for i, m in enumerate(model_ids) if m in eligible]
                mase_matrix = mase_matrix[:, keep]
                model_ids = [model_ids[i] for i in keep]
                logger.info(
                    f"Global eligibility (tau={self.DEFAULT_PARTICIPATION_TAU}): excluded "
                    f"{len(excluded)} model(s) not fully participating: {sorted(excluded)}"
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
        frequency: Optional[timedelta] = None,
        horizon: Optional[timedelta] = None,
        score_cutoff_date: Optional[date] = None,
        metric: str = "mase"
    ) -> Tuple[np.ndarray, List[int], List[int]]:
        """
        Build pivot matrix: rows=round_id matches, cols=model_id, values=AVG(metric).
        
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
        # Aggregate the selected metric per round and model (average across series in a round)
        col = self._metric_column(metric)
        base_query = f"""
            SELECT fs.round_id, fs.model_id, AVG(fs.{col}) as avg_metric
            FROM forecasts.scores fs
            JOIN challenges.rounds cr ON fs.round_id = cr.id
            WHERE fs.final_evaluation = TRUE
              AND cr.is_cancelled = FALSE
              AND fs.{col} IS NOT NULL
              AND fs.{col} != 'NaN'
              AND fs.{col} != 'Infinity'
              AND fs.{col} != '-Infinity'
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
                    WHERE frequency = :frequency
                      AND horizon = :horizon
                )
            """
            params["frequency"] = frequency
            params["horizon"] = horizon

        if score_cutoff_date is not None:
            base_query += " AND cr.registration_start::date <= :score_cutoff_date"
            params["score_cutoff_date"] = score_cutoff_date

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


    async def _get_eligible_global_model_ids(
        self,
        score_cutoff_date: Optional[date] = None,
        metric: str = "mase",
        tau: Optional[float] = None,
    ) -> set:
        """
        Model ids eligible for the GLOBAL ranking under the full-participation rule
        (ts-arena-1). A model qualifies iff, for EVERY challenge in the universe, it has
        seen the challenge since it joined the platform AND covered >= tau of that
        challenge's rounds available since it joined. See _compute_eligible_global_models.

        Fetches the raw (model_id, definition_id, round_id, registration_start) rows of the
        valid-score population (same filters as _get_scores_matrix), then computes eligibility
        in Python so the rule stays unit-testable without a database.
        """
        col = self._metric_column(metric)
        query = f"""
            SELECT DISTINCT fs.model_id, cr.definition_id, fs.round_id, cr.registration_start
            FROM forecasts.scores fs
            JOIN challenges.rounds cr ON fs.round_id = cr.id
            WHERE fs.final_evaluation = TRUE
              AND cr.is_cancelled = FALSE
              AND fs.{col} IS NOT NULL
              AND fs.{col} != 'NaN'
              AND fs.{col} != 'Infinity'
              AND fs.{col} != '-Infinity'
              AND NOT EXISTS (
                  SELECT 1 FROM challenges.definition_series_scd2 ds
                  WHERE ds.definition_id = cr.definition_id
                    AND ds.series_id = fs.series_id
                    AND ds.is_excluded = TRUE
              )
        """
        params = {}
        if score_cutoff_date is not None:
            query += " AND cr.registration_start::date <= :score_cutoff_date"
            params["score_cutoff_date"] = score_cutoff_date

        result = await self.session.execute(text(query), params)
        rows = result.fetchall()
        return self._compute_eligible_global_models(
            rows, tau if tau is not None else self.DEFAULT_PARTICIPATION_TAU
        )

    @staticmethod
    def _compute_eligible_global_models(rows, tau: float) -> set:
        """
        Pure eligibility computation for the global ranking (no DB access, unit-testable).

        Args:
            rows: iterable of (model_id, definition_id, round_id, registration_start) tuples
                  from the valid-score population (each such round is, by construction, an
                  "available" round for its challenge — at least one model was scored in it).
            tau:  minimum per-challenge coverage fraction (0..1).

        Returns:
            set of eligible model_ids. Empty if there is no data.

        Rule: universe = all definitions present. A model's join point is its earliest
        round across ANY challenge. For every challenge in the universe the model must have
        (a) at least one available round since it joined (seen it) and (b) coverage
        n_scored / n_available_since_join >= tau. Failing any challenge -> excluded.
        """
        # available[def] = {round_id: registration_start}; scored[model][def] = {round_ids}
        available: Dict[Any, Dict[Any, Any]] = {}
        scored: Dict[Any, Dict[Any, set]] = {}
        join_ts: Dict[Any, Any] = {}

        for model_id, definition_id, round_id, reg_start in rows:
            available.setdefault(definition_id, {})[round_id] = reg_start
            scored.setdefault(model_id, {}).setdefault(definition_id, set()).add(round_id)
            prev = join_ts.get(model_id)
            if prev is None or reg_start < prev:
                join_ts[model_id] = reg_start

        universe = set(available.keys())
        if not universe:
            return set()

        eligible = set()
        for model_id, joined_at in join_ts.items():
            model_defs = scored.get(model_id, {})
            ok = True
            for def_id in universe:
                # rounds of this challenge available since the model joined
                n_available = sum(
                    1 for reg in available[def_id].values() if reg >= joined_at
                )
                if n_available == 0:
                    ok = False  # challenge not seen since join -> not yet eligible
                    break
                n_scored = len(model_defs.get(def_id, ()))
                if n_scored / n_available < tau:
                    ok = False
                    break
            if ok:
                eligible.add(model_id)
        return eligible


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
    
    async def _get_definitions_with_scores(self, metric: str = "mase") -> List[int]:
        """Get all definition_ids that have finalized scores for the given metric."""
        col = self._metric_column(metric)
        query = text(f"""
            SELECT DISTINCT cr.definition_id
            FROM forecasts.scores fs
            JOIN challenges.rounds cr ON fs.round_id = cr.id
            WHERE fs.final_evaluation = TRUE
              AND cr.is_cancelled = FALSE
              AND fs.{col} IS NOT NULL
              AND cr.definition_id IS NOT NULL
            ORDER BY cr.definition_id
        """)
        result = await self.session.execute(query)
        return [row[0] for row in result.fetchall()]
    
    @staticmethod
    def _parse_pg_interval(text_val: str) -> timedelta:
        """
        Parse a PostgreSQL interval text representation into a timedelta.
        Handles formats like '00:15:00', '1 day', '3 days', '1 day 02:00:00', etc.
        """
        days = 0
        hours = 0
        minutes = 0
        seconds = 0
        
        parts = text_val.strip().split()
        i = 0
        while i < len(parts):
            if i + 1 < len(parts) and parts[i + 1].startswith('day'):
                days = int(parts[i])
                i += 2
            elif ':' in parts[i]:
                time_parts = parts[i].split(':')
                hours = int(time_parts[0])
                minutes = int(time_parts[1])
                seconds = int(time_parts[2]) if len(time_parts) > 2 else 0
                i += 1
            else:
                i += 1
        
        return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

    async def _get_frequency_horizon_groups(self, metric: str = "mase") -> List[Tuple[str, timedelta, timedelta]]:
        """
        Get unique frequency+horizon combinations from challenges.definitions
        that have finalized scores for the given metric.

        Returns tuples of (scope_id, frequency_timedelta, horizon_timedelta).
        scope_id uses the PostgreSQL interval text format (e.g. '00:15:00::1 day')
        to match the scope_id stored in round_model_scores.
        frequency/horizon timedeltas are used as bind parameters for interval filters.
        """
        col = self._metric_column(metric)
        query = text(f"""
            SELECT DISTINCT
                cd.frequency::text AS freq,
                cd.horizon::text AS hor,
                CONCAT(cd.frequency::text, '::', cd.horizon::text) AS scope_id
            FROM challenges.definitions cd
            JOIN challenges.rounds cr ON cr.definition_id = cd.id
            JOIN forecasts.scores fs ON fs.round_id = cr.id
            WHERE fs.final_evaluation = TRUE
              AND cr.is_cancelled = FALSE
              AND fs.{col} IS NOT NULL
              AND cd.frequency IS NOT NULL
              AND cd.horizon IS NOT NULL
            ORDER BY freq, hor
        """)
        result = await self.session.execute(query)
        return [
            (row[2], self._parse_pg_interval(row[0]), self._parse_pg_interval(row[1]))
            for row in result.fetchall()
        ]
    
    async def _store_ratings(
        self,
        ratings: List[EloRating],
        scope_type: str,
        scope_id: Optional[str],
        calculation_date: date,
        metric: str = "mase"
    ) -> int:
        """
        Store ELO ratings authoritatively: the (calculation_date, scope_type,
        scope_id, metric) group is deleted and re-inserted in one transaction,
        so models that dropped out of the result set (e.g. an eligibility rule
        change between two runs of the same day) cannot survive as stale rows.
        Delete-then-insert rather than upsert: rank positions swap between
        runs, and an upsert would transiently violate the per-group
        rank_position uniqueness index mid-statement-sequence.
        Also computes and stores cumulative MASE/RMSE/SQL from round_model_scores.
        The `metric` column records which score drove this ELO ranking; the cumulative
        avg_mase/avg_rmse/avg_sql columns are populated on every row regardless.

        Returns:
            Number of rows affected
        """
        # An empty result set is left untouched rather than wiping the group:
        # it is indistinguishable from an upstream data outage, and losing a
        # published day to a transient failure is worse than keeping it.
        if not ratings:
            return 0

        # Calculate rank positions
        sorted_ratings = sorted(ratings, key=lambda x: x.elo_score, reverse=True)
        rank_map = {r.model_id: idx + 1 for idx, r in enumerate(sorted_ratings)}

        # Get cumulative MASE/RMSE/SQL for all models in this scope up to calculation_date
        model_ids = [r.model_id for r in ratings]
        mase_stats = await self._get_cumulative_mase_stats(
            model_ids=model_ids,
            scope_type=scope_type,
            scope_id=scope_id,
            up_to_date=calculation_date
        )

        await self.session.execute(
            text("""
                DELETE FROM forecasts.daily_rankings
                WHERE calculation_date = :calculation_date
                  AND scope_type = :scope_type
                  AND COALESCE(scope_id, '') = COALESCE(:scope_id, '')
                  AND metric = :metric
            """),
            {
                "calculation_date": calculation_date,
                "scope_type": scope_type,
                "scope_id": scope_id,
                "metric": metric,
            },
        )

        query = text("""
            INSERT INTO forecasts.daily_rankings
                (calculation_date, model_id, scope_type, scope_id, metric,
                 elo_rating_median, elo_ci_lower, elo_ci_upper,
                 matches_played, rank_position, n_bootstraps, calculation_duration_ms,
                 avg_mase, mase_std, avg_rmse, avg_sql, sql_std, evaluated_count, calculated_at)
            VALUES
                (:calculation_date, :model_id, :scope_type, :scope_id, :metric,
                 :elo_rating_median, :elo_ci_lower, :elo_ci_upper,
                 :matches_played, :rank_position, :n_bootstraps, :calculation_duration_ms,
                 :avg_mase, :mase_std, :avg_rmse, :avg_sql, :sql_std, :evaluated_count, NOW())
        """)

        for rating in ratings:
            stats = mase_stats.get(rating.model_id, {})
            params = {
                "calculation_date": calculation_date,
                "model_id": rating.model_id,
                "scope_type": scope_type,
                "scope_id": scope_id,
                "metric": metric,
                "elo_rating_median": rating.elo_score,
                "elo_ci_lower": rating.elo_ci_lower,
                "elo_ci_upper": rating.elo_ci_upper,
                "matches_played": rating.n_matches,
                "rank_position": rank_map.get(rating.model_id),
                "n_bootstraps": rating.n_bootstraps,
                "calculation_duration_ms": rating.calculation_duration_ms,
                "avg_mase": stats.get("avg_mase"),
                "mase_std": stats.get("mase_std"),
                "avg_rmse": stats.get("avg_rmse"),
                "avg_sql": stats.get("avg_sql"),
                "sql_std": stats.get("sql_std"),
                "evaluated_count": stats.get("evaluated_count", 0)
            }
            await self.session.execute(query, params)

        await self.session.commit()
        return len(ratings)
    
    async def _get_cumulative_mase_stats(
        self,
        model_ids: List[int],
        scope_type: str,
        scope_id: Optional[str],
        up_to_date: date
    ) -> Dict[int, Dict[str, Any]]:
        """
        Get cumulative MASE/RMSE/SQL stats for models up to a given date.

        Returns:
            Dict mapping model_id -> {avg_mase, mase_std, avg_rmse, avg_sql, sql_std,
            evaluated_count}. SQL is aggregated over its own non-NULL count (num_sql), which
            can be < num_scores in the rare mae_naive==0 edge case where sql_score is NULL.
        """
        if not model_ids:
            return {}

        # Query cumulative sums from round_model_scores
        query = text("""
            SELECT
                model_id,
                SUM(sum_mase) as total_mase,
                SUM(sum_mase_sq) as total_mase_sq,
                SUM(sum_rmse) as total_rmse,
                SUM(sum_sql) as total_sql,
                SUM(sum_sql_sq) as total_sql_sq,
                SUM(num_sql) as total_sql_count,
                SUM(num_scores) as total_scores
            FROM forecasts.round_model_scores
            WHERE model_id = ANY(:model_ids)
              AND scope_type = :scope_type
              AND scope_id IS NOT DISTINCT FROM :scope_id
              AND round_date <= :up_to_date
            GROUP BY model_id
        """)

        result = await self.session.execute(query, {
            "model_ids": model_ids,
            "scope_type": scope_type,
            "scope_id": scope_id,
            "up_to_date": up_to_date
        })

        def _f(v):
            return float(v) if v is not None else None

        def _avg_std(total, total_sq, count):
            """Mean and population std from running sums; std None when count <= 1."""
            if not count or count <= 0:
                return None, None
            avg = total / count
            std = None
            if count > 1 and total_sq is not None:
                variance = (total_sq / count) - (avg ** 2)
                if variance > 0:
                    std = variance ** 0.5
            return avg, std

        stats = {}
        for row in result.fetchall():
            (model_id, total_mase, total_mase_sq, total_rmse,
             total_sql, total_sql_sq, total_sql_count, total_scores) = row
            total_mase = _f(total_mase)
            total_mase_sq = _f(total_mase_sq)
            total_rmse = _f(total_rmse)
            total_sql = _f(total_sql)
            total_sql_sq = _f(total_sql_sq)
            total_sql_count = _f(total_sql_count)
            total_scores = _f(total_scores)

            avg_mase, mase_std = _avg_std(total_mase, total_mase_sq, total_scores)
            avg_rmse = (total_rmse / total_scores) if (total_scores and total_scores > 0) else None
            avg_sql, sql_std = _avg_std(total_sql, total_sql_sq, total_sql_count)

            stats[model_id] = {
                "avg_mase": avg_mase,
                "mase_std": mase_std,
                "avg_rmse": avg_rmse,
                "avg_sql": avg_sql,
                "sql_std": sql_std,
                "evaluated_count": int(total_scores) if total_scores else 0
            }

        return stats

    
    async def has_calculated_today(self, metric: str = "mase") -> bool:
        """
        Check if ELO ratings have already been calculated today for the given metric.

        Returns:
            True if global ELO for `metric` was calculated today, False otherwise
        """
        query = text("""
            SELECT 1 FROM forecasts.daily_rankings
            WHERE scope_type = 'global'
              AND metric = :metric
              AND calculation_date = CURRENT_DATE
            LIMIT 1
        """)
        result = await self.session.execute(query, {"metric": metric})
        return result.fetchone() is not None
    
    async def get_leaderboard(
        self,
        scope_type: str = "global",
        scope_id: Optional[str] = None,
        calculation_date: Optional[date] = None,
        limit: int = 50,
        metric: str = "mase"
    ) -> List[Dict[str, Any]]:
        """
        Get ELO leaderboard from stored daily rankings.

        Args:
            scope_type: 'global', 'definition', or 'frequency_horizon'
            scope_id: Identifier for the scope (None for global)
            calculation_date: Date to fetch (default: most recent)
            limit: Maximum number of results
            metric: 'mase' (default) or 'sql' ranking dimension

        Returns:
            List of leaderboard entries with model info
        """
        if calculation_date:
            date_filter = "dr.calculation_date = :calc_date"
            params = {"calc_date": calculation_date, "scope_type": scope_type, "limit": limit, "metric": metric}
        else:
            # Get most recent date for this scope + metric
            date_filter = """dr.calculation_date = (
                SELECT MAX(calculation_date) FROM forecasts.daily_rankings
                WHERE scope_type = :scope_type AND COALESCE(scope_id, '') = COALESCE(:scope_id, '')
                  AND metric = :metric
            )"""
            params = {"scope_type": scope_type, "scope_id": scope_id, "limit": limit, "metric": metric}

        if scope_id is None:
            scope_filter = "dr.scope_id IS NULL"
        else:
            scope_filter = "dr.scope_id = :scope_id"
            params["scope_id"] = scope_id

        query = text(f"""
            SELECT * FROM forecasts.v_daily_rankings_leaderboard dr
            WHERE dr.scope_type = :scope_type
              AND dr.metric = :metric
              AND {scope_filter}
              AND {date_filter}
            ORDER BY dr.elo_rating_median DESC
            LIMIT :limit
        """)
        
        result = await self.session.execute(query, params)
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]

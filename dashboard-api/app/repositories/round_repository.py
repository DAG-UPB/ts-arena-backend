import sys
from typing import List, Dict, Any, Optional
from datetime import datetime
import psycopg2.extras


class RoundRepository:
    """Repository for Round data (ported from arena-app/src/database.py)."""
    
    def __init__(self, conn):
        self.conn = conn

    def get_round_meta(self, round_id: int) -> Optional[Dict[str, Any]]:
        """Fetch metadata for a round."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    c.id as round_id,
                    c.name,
                    c.description,
                    c.status,
                    c.context_length,
                    c.horizon,
                    c.start_time,
                    c.end_time,
                    c.registration_start,
                    c.registration_end
                FROM challenges.v_rounds_with_status c 
                WHERE c.id = %s
                """,
                (round_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
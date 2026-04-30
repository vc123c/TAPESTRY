from __future__ import annotations

from fastapi import APIRouter

from db.connection import get_read_connection

router = APIRouter(prefix="/api/conflicts", tags=["conflicts"])


@router.get("")
def get_conflicts():
    with get_read_connection() as con:
        rows = con.execute("SELECT * FROM conflict_states ORDER BY assessment_date DESC").fetchall()
    return [
        {
            "conflict_id": r[0], "assessment_date": r[1], "conflict_name": r[2], "start_date": r[3],
            "current_stage": r[4], "stage_probabilities": {1: r[5], 2: r[6], 3: r[7], 4: r[8], 5: r[9]},
            "escalation_trap_probability": r[10], "days_in_conflict": r[11], "latest_signal": r[17],
        }
        for r in rows
    ]

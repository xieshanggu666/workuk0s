"""统一账本事件、重放与对账 API。

监管可查看全量事件；核查员只读。事件日志为只追加投影，接口不提供修改/删除。
对账默认先幂等补齐旧库事件投影，但不会修改配额账户、订单、竞价或履约权威表。
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import require_roles
from app.models import User
from app.services.ledger_replay import (
    backfill_legacy_ledger_events,
    list_events,
    reconcile_ledger,
    replay_accounts,
    serialize_event,
)

router = APIRouter(prefix="/api/ledger", tags=["ledger"])


@router.get("/events")
def get_events(
    company_id: int | None = None,
    year: int | None = None,
    account_id: int | None = None,
    trace_key: str | None = None,
    ref_type: str | None = None,
    ref_id: int | None = None,
    kind: str | None = None,
    limit: int = 200,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "verifier")),
):
    events = list_events(
        db,
        company_id=company_id,
        year=year,
        account_id=account_id,
        trace_key=trace_key,
        ref_type=ref_type,
        ref_id=ref_id,
        kind=kind,
        limit=limit,
    )
    return [serialize_event(e) for e in events]


@router.get("/replay")
def replay(
    company_id: int | None = None,
    year: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "verifier")),
):
    return replay_accounts(db, company_id=company_id, year=year)


@router.get("/reconcile")
def reconcile(
    company_id: int | None = None,
    year: int | None = None,
    auto_backfill: bool = True,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "verifier")),
):
    """只读对账；auto_backfill=true 时仅幂等补齐事件投影。"""
    return reconcile_ledger(db, company_id=company_id, year=year, auto_backfill=auto_backfill)


@router.post("/backfill")
def backfill(
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin")),
):
    """监管显式回填旧账事件；可重复调用，重复事件由 event_key 去重。"""
    return backfill_legacy_ledger_events(db, commit=True)

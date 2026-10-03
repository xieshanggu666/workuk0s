"""账本重放服务：把统一事件流重放为账户投影，并维护检查点。

两种重放方式：

- **全量重放**（``replay_all`` / ``replay_account``）：从空投影开始，按
  ``(occurred_at, seq)`` 全序应用全部余额事件，得到账户的（持仓/冻结/占用）。
  用于对账、审计演示与检查点重建；
- **增量重放**（``replay_from_checkpoint``）：从该账户最近检查点出发，只应用
  ``seq > last_seq`` 的事件，供 API 快速查看“事件 → 余额”的演化。

重放规则全部取自 :mod:`app.core.event_semantics`（单一事实来源），与业务实时
记账语义天然一致；每应用一条带三余额快照的事件，可顺带校验“重放余额 == 事件
落账时快照”，快照不符只可能源于旧数据修复/库外改动，记入差异而非中断重放。

事件链完整性（seq 连续 + prev_hash/chain_hash 勾连）单独由对账器校验。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy.orm import Session

from app.core.event_semantics import apply_vector, semantics_for
from app.models.allowance import AllowanceAccount
from app.models.ledger import LedgerCheckpoint, LedgerEvent

_EPS = 1e-6


@dataclass
class ReplayState:
    """单账户重放投影。"""

    account_id: int
    current: float = 0.0
    frozen: float = 0.0
    reserved: float = 0.0
    last_seq: int = 0
    events_applied: int = 0
    # 逐笔快照不符：(事件 id, 事件类型, 重放 current/frozen/reserved, 快照三元组)
    snapshot_mismatches: list[tuple] = field(default_factory=list)
    unknown_types: set[str] = field(default_factory=set)

    def apply(self, event: LedgerEvent, *, check_snapshot: bool = True) -> None:
        sem = semantics_for(event.event_type)
        if sem.domain == "unknown":
            self.unknown_types.add(event.event_type)
        dc, df, dr = apply_vector(event.event_type, float(event.amount or 0))
        self.current = round(self.current + dc, 4)
        self.frozen = round(self.frozen + df, 4)
        self.reserved = round(self.reserved + dr, 4)
        self.last_seq = max(self.last_seq, int(event.seq))
        self.events_applied += 1

        if check_snapshot and event.account_id is not None:
            snap = _snapshot_from_payload(event)
            if snap is not None:
                sc, sf, sr = snap
                if (
                    abs(sc - self.current) > _EPS
                    or abs(sf - self.frozen) > _EPS
                    or abs(sr - self.reserved) > _EPS
                ):
                    self.snapshot_mismatches.append(
                        (event.id, event.event_type, event.is_legacy,
                         (self.current, self.frozen, self.reserved), (sc, sf, sr))
                    )


def _snapshot_from_payload(event: LedgerEvent) -> tuple[float, float, float] | None:
    """从事件 payload 取落账时三余额快照；状态事件/无快照事件返回 None。"""
    import json

    try:
        data = json.loads(event.payload or "{}")
    except ValueError:
        return None
    if "balance_after" not in data:
        return None
    return (
        round(float(data.get("balance_after", 0)), 4),
        round(float(data.get("frozen_after", 0)), 4),
        round(float(data.get("reserved_after", 0)), 4),
    )


def _balance_events(
    db: Session,
    *,
    account_id: int | None = None,
    after_seq: int | None = None,
    company_id: int | None = None,
    year: int | None = None,
):
    q = db.query(LedgerEvent).filter(LedgerEvent.direction != "status")
    if account_id is not None:
        q = q.filter(LedgerEvent.account_id == account_id)
    if after_seq is not None:
        q = q.filter(LedgerEvent.seq > after_seq)
    if company_id is not None:
        q = q.filter(LedgerEvent.company_id == company_id)
    if year is not None:
        q = q.filter(LedgerEvent.year == year)
    # 全序：事件登记的全局 seq（同事务内连续，链序即因果序）
    return q.order_by(LedgerEvent.seq.asc(), LedgerEvent.id.asc())


def replay_account(
    db: Session,
    account_id: int,
    *,
    check_snapshots: bool = True,
) -> ReplayState:
    """从空投影全量重放单个账户的全部余额事件。"""
    state = ReplayState(account_id=account_id)
    for event in _balance_events(db, account_id=account_id).all():
        state.apply(event, check_snapshot=check_snapshots)
    return state


def replay_all(
    db: Session,
    *,
    company_id: int | None = None,
    year: int | None = None,
    check_snapshots: bool = True,
) -> dict[int, ReplayState]:
    """全量重放（可按企业/年度过滤），返回 ``{account_id: ReplayState}``。"""
    states: dict[int, ReplayState] = {}
    q = db.query(LedgerEvent).filter(LedgerEvent.direction != "status")
    if company_id is not None:
        q = q.filter(LedgerEvent.company_id == company_id)
    if year is not None:
        q = q.filter(LedgerEvent.year == year)
    for event in q.order_by(LedgerEvent.seq.asc(), LedgerEvent.id.asc()).all():
        state = states.get(event.account_id)
        if state is None:
            state = ReplayState(account_id=event.account_id)
            states[event.account_id] = state
        state.apply(event, check_snapshot=check_snapshots)
    return states


def replay_from_checkpoint(db: Session, account_id: int) -> ReplayState:
    """从最近检查点增量重放（无检查点则全量）。"""
    cp = (
        db.query(LedgerCheckpoint)
        .filter(LedgerCheckpoint.account_id == account_id)
        .first()
    )
    if cp is None:
        return replay_account(db, account_id)
    state = ReplayState(
        account_id=account_id,
        current=float(cp.current_balance),
        frozen=float(cp.frozen_balance),
        reserved=float(cp.reserved_balance),
        last_seq=int(cp.last_seq),
    )
    for event in _balance_events(db, account_id=account_id, after_seq=cp.last_seq).all():
        state.apply(event)
    return state


def upsert_checkpoint(db: Session, account_id: int, state: ReplayState, *, commit: bool = False) -> LedgerCheckpoint:
    """把账户投影写为检查点（upsert，每账户一行）。"""
    account = db.get(AllowanceAccount, account_id)
    cp = (
        db.query(LedgerCheckpoint)
        .filter(LedgerCheckpoint.account_id == account_id)
        .first()
    )
    last_event = (
        db.query(LedgerEvent.chain_hash)
        .filter(LedgerEvent.account_id == account_id, LedgerEvent.seq <= state.last_seq)
        .order_by(LedgerEvent.seq.desc())
        .first()
    )
    chain_hash = last_event[0] if last_event else ""
    if cp is None:
        cp = LedgerCheckpoint(account_id=account_id)
        db.add(cp)
    cp.company_id = account.company_id if account else cp.company_id
    cp.year = account.year if account else cp.year
    cp.last_seq = state.last_seq
    cp.current_balance = round(state.current, 4)
    cp.frozen_balance = round(state.frozen, 4)
    cp.reserved_balance = round(state.reserved, 4)
    cp.chain_hash = chain_hash
    cp.updated_at = datetime.utcnow()
    db.flush()
    if commit:
        db.commit()
    return cp


def rebuild_checkpoints(db: Session, *, commit: bool = True) -> dict[str, int]:
    """全量重放后重建全部账户检查点（幂等，可随时安全重建，不动事件链）。

    检查点是可丢弃的派生数据：本函数在对账发现检查点陈旧、或旧库回填事件后
    调用，使后续增量重放/对账从最新投影起步。
    """
    states = replay_all(db, check_snapshots=False)
    for account_id, state in states.items():
        upsert_checkpoint(db, account_id, state)
    # 已无任何事件的账户：检查点归零
    known = set(states)
    for cp in db.query(LedgerCheckpoint).all():
        if cp.account_id not in known:
            cp.last_seq = 0
            cp.current_balance = 0
            cp.frozen_balance = 0
            cp.reserved_balance = 0
            cp.chain_hash = ""
            cp.updated_at = datetime.utcnow()
    db.flush()
    if commit:
        db.commit()
    return {"accounts": len(states)}


def replay_events_timeline(
    db: Session,
    *,
    company_id: int | None = None,
    year: int | None = None,
    account_id: int | None = None,
    limit: int = 200,
    before_seq: int | None = None,
) -> list[dict]:
    """事件 → 逐笔余额演化（可追溯链路的查询面，含状态事件）。

    从空投影按序计算每条事件后的累计余额，供 API/前端展示“每一笔业务发生后，
    持仓/冻结/占用如何变化”。
    """
    q = db.query(LedgerEvent)
    if company_id is not None:
        q = q.filter(LedgerEvent.company_id == company_id)
    if year is not None:
        q = q.filter(LedgerEvent.year == year)
    if account_id is not None:
        q = q.filter(LedgerEvent.account_id == account_id)
    if before_seq is not None:
        q = q.filter(LedgerEvent.seq < before_seq)
    events = q.order_by(LedgerEvent.seq.desc(), LedgerEvent.id.desc()).limit(limit).all()
    events = list(reversed(events))
    running: dict[int, list[float]] = {}
    timeline: list[dict] = []
    for event in events:
        if event.account_id is not None:
            acc = running.setdefault(event.account_id, [0.0, 0.0, 0.0])
            dc, df, dr = apply_vector(event.event_type, float(event.amount or 0))
            acc[0] = round(acc[0] + dc, 4)
            acc[1] = round(acc[1] + df, 4)
            acc[2] = round(acc[2] + dr, 4)
            cur, frz, rsv = acc
        else:
            cur = frz = rsv = None
        timeline.append({
            "seq": event.seq,
            "source": event.source,
            "source_ref": event.source_ref,
            "event_type": event.event_type,
            "direction": event.direction,
            "domain": semantics_for(event.event_type).domain,
            "amount": round(float(event.amount or 0), 4),
            "account_id": event.account_id,
            "company_id": event.company_id,
            "year": event.year,
            "trade_order_id": event.trade_order_id,
            "auction_trade_id": event.auction_trade_id,
            "is_legacy": bool(event.is_legacy),
            "event_group": event.event_group,
            "balance_after": cur,
            "frozen_after": frz,
            "reserved_after": rsv,
            "chain_hash": event.chain_hash[:12],
            "occurred_at": event.occurred_at.isoformat() if event.occurred_at else None,
        })
    return timeline

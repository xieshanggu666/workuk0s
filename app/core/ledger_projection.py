"""统一账本事件投影：把权威业务表增量投影为只追加的 ``ledger_events``。

投影过程不改变余额/业务状态，只记录可重放事实：

- ``allowance_transactions`` 是配额账本的权威流水，逐笔投影为 movement 事件，
  并在事件中展开带符号的 current/frozen/reserved 增量；
- 企业订单、竞价场次/报价/成交、履约记录、冲正批次等状态行投影为 state 事件；
- 事件键由业务主键和状态确定性生成，因此双击、超时重试、重复迁移都幂等；
- 旧库没有事件表时由迁移脚本回填，迁移后的新事务由 :func:`transactional`
  在提交前自动投影。

投影必须处于业务事务内：事件与流水/状态同生共死。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

from sqlalchemy.orm import Session

from app.models.allowance import (
    AllowanceAccount,
    AllowanceTransaction,
    ComplianceRecord,
    Quota,
    TradeOrder,
)
from app.models.auction import (
    AuctionBid,
    AuctionDefaultRepayment,
    AuctionReversalBatch,
    AuctionSession,
    AuctionTrade,
    AuctionTradeReversal,
)
from app.models.ledger import LedgerEvent
from app.models.report import MrvReport

# 持仓（current_balance）带符号方向。冻结/占用有自己的增量映射。
_CURRENT_POSITIVE = {
    "allocation",
    "buy",
    "transfer_in",
    "reversal",
    "trade_deliver_in",
    "auction_deliver_in",
    "auction_clear_refund",
    "auction_clawback_in",
    "auction_default_repay_in",
}
_CURRENT_NEGATIVE = {
    "sell",
    "transfer_out",
    "offset",
    "clear",
    "frozen_clear",
    "trade_deficit_clear",
    "auction_deficit_clear",
    "trade_deliver_out",
    "auction_deliver_out",
    "auction_clawback_out",
    "auction_default_repay_out",
}
_FROZEN_POSITIVE = {"freeze"}
_FROZEN_NEGATIVE = {
    "frozen_clear",
    "reversal_unfreeze",
    "auction_clear_unfreeze",
}
_RESERVED_POSITIVE = {
    "trade_reserve",
    "auction_bid_reserve",
    "auction_reserve",
}
_RESERVED_NEGATIVE = {
    "trade_release",
    "trade_deliver_out",
    "auction_bid_release",
    "auction_reserve_release",
    "auction_deliver_out",
}

# 旧版本可能出现的别名/预留类型也纳入映射，保证旧流水可重放。
_CURRENT_POSITIVE |= {"auction_reverse_in"}
_CURRENT_NEGATIVE |= {"auction_reverse_out"}


def _num(value: Any) -> float:
    return round(float(value or 0.0), 4)


def movement_effects(tx_type: str, amount: float) -> tuple[float, float, float]:
    """返回流水类型对应的 ``(持仓, 冻结, 占用)`` 带符号增量。"""
    amount = _num(amount)
    current = 0.0
    if tx_type in _CURRENT_POSITIVE:
        current = amount
    elif tx_type in _CURRENT_NEGATIVE:
        current = -amount

    frozen = 0.0
    if tx_type in _FROZEN_POSITIVE:
        frozen = amount
    elif tx_type in _FROZEN_NEGATIVE:
        frozen = -amount

    reserved = 0.0
    if tx_type in _RESERVED_POSITIVE:
        reserved = amount
    elif tx_type in _RESERVED_NEGATIVE:
        reserved = -amount
    return current, frozen, reserved


def _domain_for_tx(tx: AllowanceTransaction) -> str:
    if tx.auction_trade_id:
        return "auction"
    if tx.trade_order_id:
        return "trade_order"
    if tx.tx_type in {"clear", "frozen_clear", "reversal", "reversal_unfreeze"}:
        return "compliance"
    if tx.tx_type == "allocation":
        return "quota"
    return "quota"


def _trace_for_company_year(company_id: int | None, year: int | None) -> str:
    if company_id and year:
        return f"company-year:{company_id}:{year}"
    return ""


def _event(
    *,
    event_key: str,
    kind: str,
    domain: str,
    event_type: str,
    status: str,
    trace_key: str = "",
    company_id: int | None = None,
    year: int | None = None,
    account_id: int | None = None,
    transaction_id: int | None = None,
    ref_type: str = "",
    ref_id: int | None = None,
    trade_order_id: int | None = None,
    auction_session_id: int | None = None,
    auction_bid_id: int | None = None,
    auction_trade_id: int | None = None,
    compliance_record_id: int | None = None,
    report_id: int | None = None,
    amount: float = 0.0,
    frozen_delta: float = 0.0,
    reserved_delta: float = 0.0,
    balance_after: float = 0.0,
    frozen_after: float = 0.0,
    reserved_after: float = 0.0,
    source: str = "live",
    detail: str = "",
    idempotency_key: str | None = None,
    occurred_at: datetime | None = None,
) -> LedgerEvent:
    return LedgerEvent(
        event_key=event_key,
        trace_key=trace_key,
        kind=kind,
        domain=domain,
        event_type=event_type,
        status=status,
        company_id=company_id,
        year=year,
        account_id=account_id,
        transaction_id=transaction_id,
        ref_type=ref_type,
        ref_id=ref_id,
        trade_order_id=trade_order_id,
        auction_session_id=auction_session_id,
        auction_bid_id=auction_bid_id,
        auction_trade_id=auction_trade_id,
        compliance_record_id=compliance_record_id,
        report_id=report_id,
        amount=_num(amount),
        frozen_delta=_num(frozen_delta),
        reserved_delta=_num(reserved_delta),
        balance_after=_num(balance_after),
        frozen_after=_num(frozen_after),
        reserved_after=_num(reserved_after),
        source=source,
        detail=(detail or "")[:500],
        idempotency_key=idempotency_key,
        occurred_at=occurred_at or datetime.utcnow(),
    )


def _movement_event(db: Session, tx: AllowanceTransaction, *, source: str = "live") -> LedgerEvent:
    current_delta, frozen_delta, reserved_delta = movement_effects(tx.tx_type, tx.amount)
    trade = None
    if tx.auction_trade_id:
        trade = db.get(AuctionTrade, tx.auction_trade_id)

    if trade is not None:
        trace_key = f"auction_trade:{trade.id}"
        ref_type, ref_id = "auction_trade", trade.id
    elif tx.trade_order_id:
        trace_key = f"trade_order:{tx.trade_order_id}"
        ref_type, ref_id = "trade_order", tx.trade_order_id
    elif tx.tx_type in {"clear", "frozen_clear", "reversal", "reversal_unfreeze"}:
        trace_key = f"compliance:{tx.company_id}:{_tx_year(db, tx)}"
        ref_type, ref_id = "company_year", None
    else:
        trace_key = f"company-year:{tx.company_id}:{_tx_year(db, tx)}"
        ref_type, ref_id = "allowance_transaction", tx.id

    year = _tx_year(db, tx)
    return _event(
        event_key=f"allowance_transaction:{tx.id}",
        kind="movement",
        domain=_domain_for_tx(tx),
        event_type=tx.tx_type,
        status="posted",
        trace_key=trace_key,
        company_id=tx.company_id,
        year=year,
        account_id=tx.account_id,
        transaction_id=tx.id,
        ref_type=ref_type,
        ref_id=ref_id,
        trade_order_id=tx.trade_order_id,
        auction_session_id=trade.session_id if trade else None,
        auction_bid_id=None,
        auction_trade_id=tx.auction_trade_id,
        amount=current_delta,
        frozen_delta=frozen_delta,
        reserved_delta=reserved_delta,
        balance_after=tx.balance_after,
        frozen_after=tx.frozen_after,
        reserved_after=tx.reserved_after,
        source=source,
        detail=tx.remark or "",
        idempotency_key=tx.idempotency_key,
        occurred_at=tx.created_at or datetime.utcnow(),
    )


def _tx_year(db: Session, tx: AllowanceTransaction) -> int | None:
    account = db.get(AllowanceAccount, tx.account_id) if tx.account_id else None
    return account.year if account else None


def _state_payload(obj: Any) -> dict | None:
    """把支持的领域状态行转换成统一 state 事件字段。"""
    if isinstance(obj, TradeOrder):
        return {
            "event_key": f"trade_order:{obj.id}:{obj.status}",
            "domain": "trade_order",
            "event_type": "order_status",
            "status": obj.status,
            "trace_key": f"trade_order:{obj.id}",
            "company_id": None,
            "year": obj.year,
            "ref_type": "trade_order",
            "ref_id": obj.id,
            "trade_order_id": obj.id,
            "detail": f"企业订单 {obj.order_no} 状态={obj.status}",
            "occurred_at": obj.delivered_at or obj.cancelled_at or obj.confirmed_at or obj.created_at,
        }
    if isinstance(obj, AuctionSession):
        return {
            "event_key": f"auction_session:{obj.id}:{obj.status}",
            "domain": "auction",
            "event_type": "session_status",
            "status": obj.status,
            "trace_key": f"auction_session:{obj.id}",
            "company_id": None,
            "year": obj.year,
            "ref_type": "auction_session",
            "ref_id": obj.id,
            "auction_session_id": obj.id,
            "detail": f"竞价场次 {obj.session_no} 状态={obj.status}",
            "occurred_at": obj.settled_at or obj.matched_at or obj.cancelled_at or obj.open_at or obj.created_at,
        }
    if isinstance(obj, AuctionBid):
        return {
            "event_key": f"auction_bid:{obj.id}:{obj.status}",
            "domain": "auction",
            "event_type": "bid_status",
            "status": obj.status,
            "trace_key": f"auction_bid:{obj.id}",
            "company_id": obj.company_id,
            "year": obj.year,
            "ref_type": "auction_bid",
            "ref_id": obj.id,
            "auction_session_id": obj.session_id,
            "auction_bid_id": obj.id,
            "detail": f"报价 {obj.bid_no} 状态={obj.status}",
            "occurred_at": obj.cancelled_at or obj.matched_at or obj.created_at,
        }
    if isinstance(obj, AuctionTrade):
        return {
            "event_key": f"auction_trade:{obj.id}:{obj.status}",
            "domain": "auction",
            "event_type": "trade_status",
            "status": obj.status,
            "trace_key": f"auction_trade:{obj.id}",
            # 成交/冲正涉及买卖双方，状态事件不归属单一公司；双方 movement 仍各自可查。
            "company_id": None,
            "year": obj.year,
            "ref_type": "auction_trade",
            "ref_id": obj.id,
            "auction_session_id": obj.session_id,
            "auction_trade_id": obj.id,
            "detail": (
                f"成交 {obj.trade_no} 状态={obj.status}，"
                f"已冲正 {_num(obj.reversed_quantity)}/{_num(obj.quantity)}"
            ),
            "occurred_at": obj.settled_at or obj.cancelled_at or obj.created_at,
        }
    if isinstance(obj, ComplianceRecord):
        return {
            "event_key": f"compliance_record:{obj.id}:{obj.status}:active={obj.is_active}",
            "domain": "compliance",
            "event_type": "compliance_status",
            "status": obj.status if obj.is_active else "archived",
            "trace_key": f"compliance:{obj.company_id}:{obj.year}",
            "company_id": obj.company_id,
            "year": obj.year,
            "ref_type": "compliance_record",
            "ref_id": obj.id,
            "compliance_record_id": obj.id,
            "report_id": obj.report_id,
            "detail": (
                f"履约记录 {obj.company_id}/{obj.year} 状态={obj.status} "
                f"cleared={_num(obj.cleared_amount)} frozen={_num(obj.frozen_amount)} "
                f"deficit={_num(obj.deficit)}"
            ),
            "occurred_at": obj.cleared_at or datetime.utcnow(),
        }
    if isinstance(obj, MrvReport):
        return {
            "event_key": f"mrv_report:{obj.id}:{obj.status}",
            "domain": "compliance",
            "event_type": "report_status",
            "status": obj.status,
            "trace_key": f"compliance:{obj.company_id}:{obj.year}",
            "company_id": obj.company_id,
            "year": obj.year,
            "ref_type": "mrv_report",
            "ref_id": obj.id,
            "report_id": obj.id,
            "detail": f"MRV 报告 {obj.year} 状态={obj.status}",
            "occurred_at": obj.reversed_at or obj.approved_at or obj.submitted_at or obj.generated_at,
        }
    if isinstance(obj, Quota):
        return {
            "event_key": f"quota:{obj.id}:{obj.status}",
            "domain": "quota",
            "event_type": "quota_status",
            "status": obj.status,
            "trace_key": f"quota:{obj.company_id}:{obj.year}",
            "company_id": obj.company_id,
            "year": obj.year,
            "ref_type": "quota",
            "ref_id": obj.id,
            "detail": f"配额 {obj.company_id}/{obj.year} 状态={obj.status}",
            "occurred_at": obj.allocated_at or datetime.utcnow(),
        }
    if isinstance(obj, AuctionReversalBatch):
        from sqlalchemy.orm import object_session

        db = object_session(obj)
        session = db.get(AuctionSession, obj.session_id) if db is not None else None
        return {
            "event_key": f"auction_reversal_batch:{obj.id}",
            "domain": "reversal",
            "event_type": "auction_reversal_batch",
            "status": "posted",
            "trace_key": f"auction_session:{obj.session_id}",
            "year": session.year if session is not None else None,
            "ref_type": "auction_reversal_batch",
            "ref_id": obj.id,
            "auction_session_id": obj.session_id,
            "amount": obj.reverse_volume,
            "detail": f"冲正批次 {obj.batch_no}：{obj.reason}",
            "idempotency_key": obj.idempotency_key,
            "occurred_at": obj.created_at,
        }
    if isinstance(obj, AuctionTradeReversal):
        return {
            "event_key": f"auction_trade_reversal:{obj.id}",
            "domain": "reversal",
            "event_type": "auction_trade_reversal",
            "status": "posted",
            "trace_key": f"auction_trade:{obj.trade_id}",
            # 成交/冲正涉及买卖双方，状态事件不归属单一公司；双方 movement 仍各自可查。
            "company_id": None,
            "year": obj.year,
            "ref_type": "auction_trade_reversal",
            "ref_id": obj.id,
            "auction_session_id": obj.session_id,
            "auction_trade_id": obj.trade_id,
            "amount": obj.quantity,
            "detail": (
                f"成交冲正 {_num(obj.quantity)}：退还补缴 {_num(obj.clear_refunded)}，"
                f"收回 {_num(obj.recovered_quantity)}，违约 {_num(obj.defaulted_quantity)}"
            ),
            "occurred_at": obj.created_at,
        }
    if isinstance(obj, AuctionDefaultRepayment):
        return {
            "event_key": f"auction_default_repayment:{obj.id}",
            "domain": "reversal",
            "event_type": "auction_default_repayment",
            "status": "posted",
            "trace_key": f"auction_trade:{obj.trade_id}",
            # 成交/冲正涉及买卖双方，状态事件不归属单一公司；双方 movement 仍各自可查。
            "company_id": None,
            "year": obj.year,
            "ref_type": "auction_default_repayment",
            "ref_id": obj.id,
            "auction_session_id": obj.session_id,
            "auction_trade_id": obj.trade_id,
            "amount": obj.quantity,
            "detail": f"违约追偿 {obj.source} {_num(obj.quantity)}",
            "idempotency_key": obj.idempotency_key,
            "occurred_at": obj.created_at,
        }
    return None


def _supported_state_object(obj: Any) -> bool:
    return isinstance(
        obj,
        (
            TradeOrder,
            AuctionSession,
            AuctionBid,
            AuctionTrade,
            ComplianceRecord,
            MrvReport,
            Quota,
            AuctionReversalBatch,
            AuctionTradeReversal,
            AuctionDefaultRepayment,
        ),
    )


def _referenced_state_objects(db: Session, txs: Iterable[AllowanceTransaction]) -> list[Any]:
    objects: list[Any] = []
    seen: set[tuple[str, int]] = set()

    def add(ref_type: str, obj: Any | None) -> None:
        if obj is None or not getattr(obj, "id", None):
            return
        key = (ref_type, int(obj.id))
        if key not in seen:
            seen.add(key)
            objects.append(obj)

    for tx in txs:
        if tx.trade_order_id:
            add("trade_order", db.get(TradeOrder, tx.trade_order_id))
        if tx.auction_trade_id:
            trade = db.get(AuctionTrade, tx.auction_trade_id)
            add("auction_trade", trade)
            if trade is not None:
                add("auction_session", db.get(AuctionSession, trade.session_id))
                add("auction_bid", db.get(AuctionBid, trade.buyer_bid_id))
                add("auction_bid", db.get(AuctionBid, trade.seller_bid_id))
        # 配额流水属于某企业年度时，同步该年度当前履约/配额状态。
        year = _tx_year(db, tx)
        if tx.company_id and year:
            record = (
                db.query(ComplianceRecord)
                .filter(
                    ComplianceRecord.company_id == tx.company_id,
                    ComplianceRecord.year == year,
                    ComplianceRecord.is_active == 1,
                )
                .first()
            )
            add("compliance_record", record)
            quota = (
                db.query(Quota)
                .filter(Quota.company_id == tx.company_id, Quota.year == year)
                .first()
            )
            add("quota", quota)
    return objects


def record_state_event(db: Session, obj: Any, *, source: str = "live") -> LedgerEvent | None:
    """在当前事务内显式记录一条领域状态事件（用于条件 UPDATE 绕过 dirty 集合的场景）。

    事件键确定性包含对象主键与状态，因此重复确认/重复结算等重试天然幂等。
    """
    payload = _state_payload(obj)
    if payload is None:
        return None
    key = payload.pop("event_key")
    exists = (
        db.query(LedgerEvent.id)
        .filter(LedgerEvent.event_key == key)
        .first()
    )
    if exists:
        return None
    event = _event(event_key=key, kind="state", source=source, **payload)
    db.add(event)
    db.flush()
    return event



def project_pending_ledger_events(db: Session, *, source: str = "live") -> int:
    """把当前事务内待提交的新流水和状态变更投影为账本事件。

    返回新增事件数。该函数会 flush 业务对象以取得主键，但不 commit；
    提交/回滚仍由外层 :func:`app.core.ledger.transactional` 控制。
    """
    pending_txs = [x for x in db.new if isinstance(x, AllowanceTransaction)]
    pending_states = [x for x in (*db.new, *db.dirty) if _supported_state_object(x)]

    if not pending_txs and not pending_states:
        return 0

    db.flush()
    pending_txs = [tx for tx in pending_txs if tx.id]
    pending_states = [obj for obj in pending_states if getattr(obj, "id", None)]

    # 新流水涉及的成交/订单/履约状态也要留下同事务状态点；尤其成交冲正等
    # 状态使用条件 UPDATE 推进，不一定出现在 SQLAlchemy dirty 集合中。
    state_objects = [*pending_states, *_referenced_state_objects(db, pending_txs)]

    candidates: list[LedgerEvent] = [_movement_event(db, tx, source=source) for tx in pending_txs]
    payloads: list[dict] = []
    seen_state_keys: set[str] = set()
    for obj in state_objects:
        payload = _state_payload(obj)
        if payload is None or payload["event_key"] in seen_state_keys:
            continue
        seen_state_keys.add(payload["event_key"])
        payloads.append(payload)

    if not candidates and not payloads:
        return 0

    existing = set(
        key
        for (key,) in db.query(LedgerEvent.event_key)
        .filter(
            LedgerEvent.event_key.in_(
                [c.event_key for c in candidates] + [p["event_key"] for p in payloads]
            )
        )
        .all()
    )
    candidates = [c for c in candidates if c.event_key not in existing]
    for payload in payloads:
        if payload["event_key"] in existing:
            continue
        kwargs = {k: v for k, v in payload.items() if k != "event_key"}
        candidates.append(_event(event_key=payload["event_key"], kind="state", source=source, **kwargs))

    if candidates:
        db.add_all(candidates)
        db.flush()
    return len(candidates)

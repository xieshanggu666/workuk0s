"""统一账本事件的重放、旧账回填与对账。

权威余额仍保存在 ``allowance_accounts``，权威业务状态保存在订单/竞价/履约等表；
``ledger_events`` 是可追溯、可重放的事件投影。该模块只做三类事情：

1. **回填（backfill）**：把旧库已有流水和当前业务状态补成 legacy/backfill 事件；
2. **重放（replay）**：按 account 分组顺序累加 movement 事件，从期初余额重建
   current/frozen/reserved，并把 state 事件作为订单/竞价/履约轨迹返回；
3. **对账（reconcile）**：检查事件链、账户快照链、履约恒等式、订单/竞价状态机、
   部分冲正/违约追偿累计以及企业年度边界。

旧记录兼容策略：不修改、不删除历史流水；每笔旧流水补一条 ``legacy`` movement，
业务当前状态补一条 ``legacy`` state。迁移前的完整状态迁移历史不可得，因此只保证
“旧账终态可回放、可对账”，不虚构历史状态。
"""

from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.ledger_projection import (
    _event,
    _state_payload,
)
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

_EPS = 1e-6


def _num(value: Any) -> float:
    return round(float(value or 0.0), 4)


def _issue(code: str, message: str, severity: str = "error", **context: Any) -> dict:
    return {"code": code, "severity": severity, "message": message, "context": context}


def _existing_keys(db: Session, keys: list[str]) -> set[str]:
    if not keys:
        return set()
    return {
        key
        for (key,) in db.query(LedgerEvent.event_key)
        .filter(LedgerEvent.event_key.in_(keys))
        .all()
    }


def _add_state_events(
    db: Session,
    objects: list[Any],
    *,
    source: str,
    existing: set[str] | None = None,
) -> list[LedgerEvent]:
    payloads: dict[str, dict] = {}
    for obj in objects:
        payload = _state_payload(obj)
        if payload is not None and payload["event_key"] not in payloads:
            payloads[payload["event_key"]] = payload

    keys = list(payloads)
    known = existing if existing is not None else _existing_keys(db, keys)
    events = []
    for key, payload in payloads.items():
        if key in known:
            continue
        kwargs = {k: v for k, v in payload.items() if k != "event_key"}
        events.append(_event(event_key=key, kind="state", source=source, **kwargs))
        known.add(key)
    if events:
        db.add_all(events)
    return events


def backfill_legacy_ledger_events(db: Session, *, commit: bool = True) -> dict:
    """回填旧库流水与领域状态，幂等可重复执行。

    新增事件分三类：
    - 旧流水：``allowance_transactions`` 中尚无事件投影的每笔流水；
    - 领域终态：订单/报价/成交/履约/报告/配额/冲正等当前状态；
    - 旧期初：``opening_balance`` 中无法由 allocation 流水解释的历史期初余额，
      补一条 ``legacy_opening_balance``，使事件链可以从零重放到账户现值。
    """
    tx_events: list[LedgerEvent] = []
    state_events: list[LedgerEvent] = []
    opening_events: list[LedgerEvent] = []

    # 1) 旧配额流水：左连接找出没有 movement 事件的流水，按时间顺序补齐。
    missing_txs = (
        db.query(AllowanceTransaction)
        .outerjoin(LedgerEvent, LedgerEvent.transaction_id == AllowanceTransaction.id)
        .filter(LedgerEvent.kind == "movement")
        .filter(LedgerEvent.id.is_(None))
        .order_by(AllowanceTransaction.id.asc())
        .all()
    )

    # 回填时不能直接调用 project_pending_ledger_events：这些 tx 不是当前事务
    # 新建对象，需要显式标记 source=legacy。
    from app.core.ledger_projection import _movement_event

    for tx in missing_txs:
        tx_events.append(_movement_event(db, tx, source="legacy"))

    # 2) 旧业务终态。迁移前状态迁移历史不可得，仅记录当前终态并明确 source=legacy。
    state_objects: list[Any] = []
    state_objects.extend(db.query(Quota).order_by(Quota.id).all())
    state_objects.extend(db.query(TradeOrder).order_by(TradeOrder.id).all())
    state_objects.extend(db.query(AuctionSession).order_by(AuctionSession.id).all())
    state_objects.extend(db.query(AuctionBid).order_by(AuctionBid.id).all())
    state_objects.extend(db.query(AuctionTrade).order_by(AuctionTrade.id).all())
    state_objects.extend(db.query(ComplianceRecord).order_by(ComplianceRecord.id).all())
    state_objects.extend(db.query(MrvReport).order_by(MrvReport.id).all())
    state_objects.extend(db.query(AuctionReversalBatch).order_by(AuctionReversalBatch.id).all())
    state_objects.extend(db.query(AuctionTradeReversal).order_by(AuctionTradeReversal.id).all())
    state_objects.extend(db.query(AuctionDefaultRepayment).order_by(AuctionDefaultRepayment.id).all())
    state_events.extend(_add_state_events(db, state_objects, source="legacy"))

    # 3) 旧期初余额：只补无法由 allocation 流水解释的部分，避免重复扩大系统总量。
    accounts = db.query(AllowanceAccount).order_by(AllowanceAccount.id).all()
    opening_keys = [f"legacy:opening_balance:{a.id}" for a in accounts]
    existing = _existing_keys(db, opening_keys)
    for account in accounts:
        key = f"legacy:opening_balance:{account.id}"
        if key in existing:
            continue
        allocation_total = (
            db.query(func.coalesce(func.sum(AllowanceTransaction.amount), 0))
            .filter(
                AllowanceTransaction.account_id == account.id,
                AllowanceTransaction.tx_type == "allocation",
            )
            .scalar()
        )
        legacy_opening = round(_num(account.opening_balance) - _num(allocation_total), 4)
        if legacy_opening <= _EPS:
            continue
        # 旧期初修正在迁移时点追加，而不是伪造在历史交易之前；这样既不破坏旧流水
        # 自身的余额快照链，又能让事件总增量闭合到当前账户。
        account_movements = [
            tx for tx in db.query(AllowanceTransaction)
            .filter(AllowanceTransaction.account_id == account.id)
            .order_by(AllowanceTransaction.created_at.asc(), AllowanceTransaction.id.asc())
            .all()
        ]
        occurred_at = (
            account_movements[-1].created_at + timedelta(seconds=1)
            if account_movements and account_movements[-1].created_at
            else None
        )
        opening_events.append(
            _event(
                event_key=key,
                kind="movement",
                domain="quota",
                event_type="legacy_opening_balance",
                status="posted",
                trace_key=f"company-year:{account.company_id}:{account.year}",
                company_id=account.company_id,
                year=account.year,
                account_id=account.id,
                ref_type="allowance_account",
                ref_id=account.id,
                amount=legacy_opening,
                balance_after=_num(account.current_balance),
                frozen_after=_num(account.frozen_balance),
                reserved_after=_num(account.reserved_balance),
                source="legacy",
                detail=f"旧账迁移：补录 {account.year} 年度历史期初余额 {legacy_opening} 吨",
                occurred_at=occurred_at,
            )
        )

    inserted = len(tx_events) + len(state_events) + len(opening_events)
    if inserted:
        db.add_all(tx_events)
        db.add_all(opening_events)
        db.flush()
        if commit:
            db.commit()

    return {
        "inserted": inserted,
        "movement_events": len(tx_events),
        "state_events": len(state_events),
        "legacy_opening_events": len(opening_events),
    }


def _account_replay_data(db: Session, account: AllowanceAccount) -> dict:
    events = (
        db.query(LedgerEvent)
        .filter(
            LedgerEvent.account_id == account.id,
            LedgerEvent.kind == "movement",
        )
        .order_by(LedgerEvent.occurred_at.asc(), LedgerEvent.id.asc())
        .all()
    )
    current = frozen = reserved = 0.0
    allocation_total = legacy_opening_total = 0.0
    for event in events:
        current = round(current + _num(event.amount), 4)
        frozen = round(frozen + _num(event.frozen_delta), 4)
        reserved = round(reserved + _num(event.reserved_delta), 4)
        if event.event_type == "allocation":
            allocation_total = round(allocation_total + _num(event.amount), 4)
        if event.event_type == "legacy_opening_balance":
            legacy_opening_total = round(legacy_opening_total + _num(event.amount), 4)

    seed = round(_num(account.opening_balance) - allocation_total - legacy_opening_total, 4)
    return {
        "account_id": account.id,
        "company_id": account.company_id,
        "year": account.year,
        "opening_balance": _num(account.opening_balance),
        "allocation_events_total": allocation_total,
        "legacy_opening_total": legacy_opening_total,
        "replay_seed": seed,
        "replayed_current": round(seed + current, 4),
        "replayed_frozen": frozen,
        "replayed_reserved": reserved,
        "event_count": len(events),
    }


def replay_accounts(
    db: Session,
    *,
    company_id: int | None = None,
    year: int | None = None,
) -> list[dict]:
    """按企业/年度过滤并重放全部账户，返回重放余额。"""
    q = db.query(AllowanceAccount)
    if company_id is not None:
        q = q.filter(AllowanceAccount.company_id == company_id)
    if year is not None:
        q = q.filter(AllowanceAccount.year == year)
    return [_account_replay_data(db, account) for account in q.order_by(AllowanceAccount.id).all()]


def list_events(
    db: Session,
    *,
    company_id: int | None = None,
    year: int | None = None,
    account_id: int | None = None,
    trace_key: str | None = None,
    ref_type: str | None = None,
    ref_id: int | None = None,
    kind: str | None = None,
    limit: int = 200,
) -> list[LedgerEvent]:
    limit = max(1, min(limit, 1000))
    q = db.query(LedgerEvent)
    if company_id is not None:
        q = q.filter(LedgerEvent.company_id == company_id)
    if year is not None:
        q = q.filter(LedgerEvent.year == year)
    if account_id is not None:
        q = q.filter(LedgerEvent.account_id == account_id)
    if trace_key:
        q = q.filter(LedgerEvent.trace_key == trace_key)
    if ref_type:
        q = q.filter(LedgerEvent.ref_type == ref_type)
    if ref_id is not None:
        q = q.filter(LedgerEvent.ref_id == ref_id)
    if kind:
        q = q.filter(LedgerEvent.kind == kind)
    return q.order_by(LedgerEvent.occurred_at.desc(), LedgerEvent.id.desc()).limit(limit).all()


def _check_movement_projection(db: Session, issues: list[dict]) -> dict:
    tx_count = db.query(func.count(AllowanceTransaction.id)).scalar() or 0
    movement_count = (
        db.query(func.count(LedgerEvent.id))
        .filter(LedgerEvent.kind == "movement", LedgerEvent.transaction_id.isnot(None))
        .scalar()
        or 0
    )
    if tx_count != movement_count:
        issues.append(_issue(
            "MOVEMENT_PROJECTION_MISMATCH",
            f"配额流水 {tx_count} 笔，但 movement 事件 {movement_count} 笔，请执行旧账回填",
            transaction_count=tx_count,
            movement_event_count=movement_count,
        ))

    duplicated_tx = (
        db.query(LedgerEvent.transaction_id, func.count(LedgerEvent.id))
        .filter(LedgerEvent.transaction_id.isnot(None))
        .group_by(LedgerEvent.transaction_id)
        .having(func.count(LedgerEvent.id) > 1)
        .all()
    )
    for transaction_id, count in duplicated_tx:
        issues.append(_issue(
            "DUPLICATE_MOVEMENT_EVENT",
            f"配额流水 {transaction_id} 存在 {count} 条 movement 事件",
            transaction_id=transaction_id,
            count=count,
        ))


def _check_event_snapshot_chain(db: Session, account_id: int, issues: list[dict]) -> None:
    """校验 movement 事件自身的增量→快照链，捕获损坏或手工篡改的事件。"""
    events = (
        db.query(LedgerEvent)
        .filter(LedgerEvent.account_id == account_id, LedgerEvent.kind == "movement")
        .order_by(LedgerEvent.occurred_at.asc(), LedgerEvent.id.asc())
        .all()
    )
    # 从“事件总增量的反推基线”开始校验：正常新库基线为 0；纯旧库若用
    # legacy_opening_balance 在迁移时点闭合期初，则真实交易事件共享该隐式期初。
    signed_current = sum(_num(e.amount) for e in events)
    last_current = _num(events[-1].balance_after) if events else 0.0
    opening_current = round(last_current - signed_current, 4)
    current = opening_current
    # 旧期初修正事件没有冻结/占用修正（仅用于闭合旧持仓），冻结/占用仍从 0 重放。
    frozen = reserved = 0.0
    for event in events:
        if event.event_type == "legacy_opening_balance":
            # 旧期初修正在迁移时点作为余额闭合点，不参与旧流水快照的连续推演。
            current = _num(event.balance_after)
        else:
            current = round(current + _num(event.amount), 4)
        frozen = round(frozen + _num(event.frozen_delta), 4)
        reserved = round(reserved + _num(event.reserved_delta), 4)
        if abs(current - _num(event.balance_after)) > _EPS:
            issues.append(_issue(
                "EVENT_CURRENT_SNAPSHOT_MISMATCH",
                f"事件 {event.event_key} 的持仓快照与事件重放不一致",
                event_key=event.event_key,
                expected=current,
                actual=_num(event.balance_after),
            ))
        if abs(frozen - _num(event.frozen_after)) > _EPS:
            issues.append(_issue(
                "EVENT_FROZEN_SNAPSHOT_MISMATCH",
                f"事件 {event.event_key} 的冻结快照与事件重放不一致",
                event_key=event.event_key,
                expected=frozen,
                actual=_num(event.frozen_after),
            ))
        if abs(reserved - _num(event.reserved_after)) > _EPS:
            issues.append(_issue(
                "EVENT_RESERVED_SNAPSHOT_MISMATCH",
                f"事件 {event.event_key} 的占用快照与事件重放不一致",
                event_key=event.event_key,
                expected=reserved,
                actual=_num(event.reserved_after),
            ))


def _check_accounts(db: Session, company_id: int | None, year: int | None, issues: list[dict]) -> None:
    q = db.query(AllowanceAccount)
    if company_id is not None:
        q = q.filter(AllowanceAccount.company_id == company_id)
    if year is not None:
        q = q.filter(AllowanceAccount.year == year)

    for account in q.order_by(AllowanceAccount.id).all():
        _check_event_snapshot_chain(db, account.id, issues)
        current = _num(account.current_balance)
        frozen = _num(account.frozen_balance)
        reserved = _num(account.reserved_balance)
        if current < -_EPS or frozen < -_EPS or reserved < -_EPS:
            issues.append(_issue(
                "NEGATIVE_BALANCE",
                f"账户 {account.id} 存在负余额",
                account_id=account.id,
                current=current,
                frozen=frozen,
                reserved=reserved,
            ))
        if current + _EPS < frozen + reserved:
            issues.append(_issue(
                "RESERVED_EXCEEDS_CURRENT",
                f"账户 {account.id} 持仓小于冻结+占用",
                account_id=account.id,
                current=current,
                frozen=frozen,
                reserved=reserved,
            ))

        replay = _account_replay_data(db, account)
        if abs(replay["replayed_current"] - current) > _EPS:
            issues.append(_issue(
                "REPLAY_CURRENT_MISMATCH",
                f"账户 {account.id} 重放持仓 {replay['replayed_current']} 与当前持仓 {current} 不一致",
                account_id=account.id,
                replayed=replay["replayed_current"],
                actual=current,
            ))
        if abs(replay["replayed_frozen"] - frozen) > _EPS:
            issues.append(_issue(
                "REPLAY_FROZEN_MISMATCH",
                f"账户 {account.id} 重放冻结 {replay['replayed_frozen']} 与当前冻结 {frozen} 不一致",
                account_id=account.id,
                replayed=replay["replayed_frozen"],
                actual=frozen,
            ))
        if abs(replay["replayed_reserved"] - reserved) > _EPS:
            issues.append(_issue(
                "REPLAY_RESERVED_MISMATCH",
                f"账户 {account.id} 重放占用 {replay['replayed_reserved']} 与当前占用 {reserved} 不一致",
                account_id=account.id,
                replayed=replay["replayed_reserved"],
                actual=reserved,
            ))
        if abs(replay["replay_seed"]) > _EPS:
            issues.append(_issue(
                "LEGACY_OPENING_NOT_CLOSED",
                f"账户 {account.id} 期初余额未被 allocation/legacy opening 事件闭合",
                account_id=account.id,
                seed=replay["replay_seed"],
            ))


def _check_compliance(db: Session, company_id: int | None, year: int | None, issues: list[dict]) -> None:
    q = db.query(ComplianceRecord)
    if company_id is not None:
        q = q.filter(ComplianceRecord.company_id == company_id)
    if year is not None:
        q = q.filter(ComplianceRecord.year == year)

    active_groups: dict[tuple[int, int], int] = defaultdict(int)
    for record in q.order_by(ComplianceRecord.id).all():
        emission = _num(record.verified_emission)
        cleared = _num(record.cleared_amount)
        frozen = _num(record.frozen_amount)
        deficit = _num(record.deficit)
        if record.is_active:
            active_groups[(record.company_id, record.year)] += 1
        if record.is_active and cleared - emission > _EPS:
            issues.append(_issue(
                "CLEARED_EXCEEDS_EMISSION",
                f"履约记录 {record.id} 清缴量超过核定排放量",
                compliance_id=record.id,
                cleared=cleared,
                emission=emission,
            ))
        if record.is_active and abs(emission - cleared - frozen - deficit) > _EPS:
            issues.append(_issue(
                "COMPLIANCE_EQUATION_BROKEN",
                f"履约记录 {record.id} 不满足 排放=清缴+冻结+缺口",
                compliance_id=record.id,
                emission=emission,
                cleared=cleared,
                frozen=frozen,
                deficit=deficit,
            ))
        expected_status = "compliant" if emission <= _EPS or deficit <= _EPS else "deficit"
        if record.is_active and record.status not in {expected_status, "pending"}:
            issues.append(_issue(
                "COMPLIANCE_STATUS_MISMATCH",
                f"履约记录 {record.id} 状态 {record.status} 与缺口 {deficit} 不一致",
                compliance_id=record.id,
                status=record.status,
                expected=expected_status,
            ))
        event = (
            db.query(LedgerEvent)
            .filter(
                LedgerEvent.ref_type == "compliance_record",
                LedgerEvent.ref_id == record.id,
            )
            .first()
        )
        if event is None:
            issues.append(_issue(
                "COMPLIANCE_TRACE_MISSING",
                f"履约记录 {record.id} 缺少账本状态事件",
                compliance_id=record.id,
            ))

    for (cid, yr), count in active_groups.items():
        if count > 1:
            issues.append(_issue(
                "MULTIPLE_ACTIVE_COMPLIANCE",
                f"企业 {cid} {yr} 年度存在 {count} 条活跃履约记录",
                company_id=cid,
                year=yr,
                count=count,
            ))


def _trade_movement_sum(db: Session, order_id: int, tx_types: set[str]) -> float:
    total = (
        db.query(func.coalesce(func.sum(AllowanceTransaction.amount), 0))
        .filter(
            AllowanceTransaction.trade_order_id == order_id,
            AllowanceTransaction.tx_type.in_(tuple(tx_types)),
        )
        .scalar()
    )
    return _num(total)


def _check_orders(db: Session, company_id: int | None, year: int | None, issues: list[dict]) -> None:
    q = db.query(TradeOrder)
    if year is not None:
        q = q.filter(TradeOrder.year == year)
    if company_id is not None:
        q = q.filter(
            (TradeOrder.seller_id == company_id) | (TradeOrder.buyer_id == company_id)
        )
    orders = q.order_by(TradeOrder.id).all()
    for order in orders:
        amount = _num(order.amount)
        reserve = _trade_movement_sum(db, order.id, {"trade_reserve"})
        release = _trade_movement_sum(db, order.id, {"trade_release"})
        deliver_out = _trade_movement_sum(db, order.id, {"trade_deliver_out"})
        outstanding_reserved = round(reserve - release - deliver_out, 4)
        if order.status == "confirmed" and abs(outstanding_reserved - amount) > _EPS:
            issues.append(_issue(
                "ORDER_RESERVE_MISMATCH",
                f"订单 {order.order_no} 已确认但未交割占用不等于订单量",
                order_id=order.id,
                reserved=outstanding_reserved,
                amount=amount,
            ))
        if order.status in {"pending", "delivered", "cancelled"} and abs(outstanding_reserved) > _EPS:
            issues.append(_issue(
                "ORDER_RESERVED_NOT_RELEASED",
                f"订单 {order.order_no} 状态 {order.status} 但仍有交易占用",
                order_id=order.id,
                reserved=outstanding_reserved,
                status=order.status,
            ))
        if order.status == "delivered":
            in_amount = _trade_movement_sum(db, order.id, {"trade_deliver_in"})
            if abs(deliver_out - amount) > _EPS or abs(in_amount - amount) > _EPS:
                issues.append(_issue(
                    "ORDER_DELIVERY_MISMATCH",
                    f"订单 {order.order_no} 已交割但出入账量不等于订单量",
                    order_id=order.id,
                    out=deliver_out,
                    in_amount=in_amount,
                    amount=amount,
                ))
        if order.year is not None:
            cross = (
                db.query(LedgerEvent)
                .filter(
                    LedgerEvent.trade_order_id == order.id,
                    LedgerEvent.year != order.year,
                )
                .first()
            )
            if cross:
                issues.append(_issue(
                    "CROSS_YEAR_ORDER_EVENT",
                    f"订单 {order.order_no} 的账本事件跨年度",
                    order_id=order.id,
                    order_year=order.year,
                    event_year=cross.year,
                ))


def _auction_tx_sum(db: Session, trade_id: int, tx_types: set[str], side: str | None = None) -> float:
    query = db.query(func.coalesce(func.sum(AllowanceTransaction.amount), 0)).filter(
        AllowanceTransaction.auction_trade_id == trade_id,
        AllowanceTransaction.tx_type.in_(tuple(tx_types)),
    )
    if side in {"buyer", "seller"}:
        query = (
            query.join(AuctionTrade, AuctionTrade.id == AllowanceTransaction.auction_trade_id)
            .join(AllowanceAccount, AllowanceAccount.id == AllowanceTransaction.account_id)
        )
        if side == "buyer":
            query = query.filter(AllowanceAccount.company_id == AuctionTrade.buyer_id)
        else:
            query = query.filter(AllowanceAccount.company_id == AuctionTrade.seller_id)
    total = query.scalar()
    return _num(total)


def _check_auctions(db: Session, company_id: int | None, year: int | None, issues: list[dict]) -> None:
    trade_q = db.query(AuctionTrade)
    if year is not None:
        trade_q = trade_q.filter(AuctionTrade.year == year)
    if company_id is not None:
        trade_q = trade_q.filter(
            (AuctionTrade.buyer_id == company_id) | (AuctionTrade.seller_id == company_id)
        )

    sessions = {s.id: s for s in db.query(AuctionSession).all()}
    grouped: dict[int, list[AuctionTrade]] = defaultdict(list)
    for trade in trade_q.order_by(AuctionTrade.id).all():
        grouped[trade.session_id].append(trade)
        quantity = _num(trade.quantity)
        reversed_qty = _num(trade.reversed_quantity)
        defaulted = _num(trade.defaulted_amount)
        repaid = _num(trade.repaid_amount)
        if reversed_qty - quantity > _EPS:
            issues.append(_issue(
                "REVERSED_EXCEEDS_TRADE",
                f"成交 {trade.trade_no} 冲正量超过成交量",
                trade_id=trade.id,
                quantity=quantity,
                reversed=reversed_qty,
            ))
        if defaulted - reversed_qty > _EPS:
            issues.append(_issue(
                "DEFAULT_EXCEEDS_REVERSED",
                f"成交 {trade.trade_no} 违约欠额超过冲正量",
                trade_id=trade.id,
                defaulted=defaulted,
                reversed=reversed_qty,
            ))
        if repaid - defaulted > _EPS:
            issues.append(_issue(
                "REPAID_EXCEEDS_DEFAULT",
                f"成交 {trade.trade_no} 追偿量超过违约欠额",
                trade_id=trade.id,
                repaid=repaid,
                defaulted=defaulted,
            ))
        if trade.status in {"settled", "defaulted", "reversed"}:
            out_sum = _auction_tx_sum(db, trade.id, {"auction_deliver_out"}, side="seller")
            in_sum = _auction_tx_sum(db, trade.id, {"auction_deliver_in"}, side="buyer")
            if abs(out_sum - quantity) > _EPS or abs(in_sum - quantity) > _EPS:
                issues.append(_issue(
                    "AUCTION_DELIVERY_MISMATCH",
                    f"成交 {trade.trade_no} 结算出入账量不等于成交量",
                    trade_id=trade.id,
                    out=out_sum,
                    in_amount=in_sum,
                    quantity=quantity,
                ))

        reversal_sum = (
            db.query(func.coalesce(func.sum(AuctionTradeReversal.quantity), 0))
            .filter(AuctionTradeReversal.trade_id == trade.id)
            .scalar()
        )
        if abs(_num(reversal_sum) - reversed_qty) > _EPS:
            issues.append(_issue(
                "PARTIAL_REVERSAL_LEDGER_MISMATCH",
                f"成交 {trade.trade_no} 冲正明细累计与成交单冲正量不一致",
                trade_id=trade.id,
                detail_sum=_num(reversal_sum),
                reversed=reversed_qty,
            ))
        repayment_sum = (
            db.query(func.coalesce(func.sum(AuctionDefaultRepayment.quantity), 0))
            .filter(AuctionDefaultRepayment.trade_id == trade.id)
            .scalar()
        )
        if abs(_num(repayment_sum) - repaid) > _EPS:
            issues.append(_issue(
                "DEFAULT_REPAYMENT_LEDGER_MISMATCH",
                f"成交 {trade.trade_no} 追偿明细累计与成交单追偿量不一致",
                trade_id=trade.id,
                detail_sum=_num(repayment_sum),
                repaid=repaid,
            ))

        cross_event = (
            db.query(LedgerEvent)
            .filter(LedgerEvent.auction_trade_id == trade.id, LedgerEvent.year != trade.year)
            .first()
        )
        if cross_event:
            issues.append(_issue(
                "CROSS_YEAR_AUCTION_EVENT",
                f"成交 {trade.trade_no} 的账本事件跨年度",
                trade_id=trade.id,
                trade_year=trade.year,
                event_year=cross_event.year,
            ))

    # 场次成交量/笔数是全市场聚合，企业维度只检查其参与成交单本身；
    # 否则拿单个企业的子集与场次总量比较会产生误报。
    if company_id is None:
        for session_id, trades in grouped.items():
            session = sessions.get(session_id) or db.get(AuctionSession, session_id)
            if session is None:
                continue
            live_trades = [t for t in trades if t.status != "cancelled"]
            total = round(sum(_num(t.quantity) for t in live_trades), 4)
            if abs(_num(session.matched_volume) - total) > _EPS:
                issues.append(_issue(
                    "SESSION_VOLUME_MISMATCH",
                    f"竞价场次 {session.session_no} 成交量汇总不一致",
                    session_id=session.id,
                    session_volume=_num(session.matched_volume),
                    trade_volume=total,
                ))
            if session.trade_count != len(live_trades):
                issues.append(_issue(
                    "SESSION_TRADE_COUNT_MISMATCH",
                    f"竞价场次 {session.session_no} 成交笔数汇总不一致",
                    session_id=session.id,
                    session_count=session.trade_count,
                    trade_count=len(live_trades),
                ))


def _check_year_boundaries(db: Session, company_id: int | None, year: int | None, issues: list[dict]) -> None:
    q = db.query(LedgerEvent).filter(LedgerEvent.account_id.isnot(None))
    if company_id is not None:
        q = q.filter(LedgerEvent.company_id == company_id)
    if year is not None:
        q = q.filter(LedgerEvent.year == year)
    for event in q.order_by(LedgerEvent.id).all():
        account = db.get(AllowanceAccount, event.account_id) if event.account_id else None
        if account is not None and event.year != account.year:
            issues.append(_issue(
                "EVENT_ACCOUNT_YEAR_MISMATCH",
                f"事件 {event.event_key} 的年度与账户年度不一致",
                event_key=event.event_key,
                event_year=event.year,
                account_year=account.year,
            ))


def reconcile_ledger(
    db: Session,
    *,
    company_id: int | None = None,
    year: int | None = None,
    auto_backfill: bool = True,
) -> dict:
    """执行统一账本对账。``auto_backfill`` 只补事件投影，不修改权威业务表。"""
    backfill = {"inserted": 0}
    # 回填在独立事务中提交；即使随后发现账实不符，也保留可审计的投影补齐记录。
    if auto_backfill:
        backfill = backfill_legacy_ledger_events(db, commit=True)

    issues: list[dict] = []
    _check_movement_projection(db, issues)
    _check_accounts(db, company_id, year, issues)
    _check_compliance(db, company_id, year, issues)
    _check_orders(db, company_id, year, issues)
    _check_auctions(db, company_id, year, issues)
    _check_year_boundaries(db, company_id, year, issues)

    if company_id is not None or year is not None:
        replay = replay_accounts(db, company_id=company_id, year=year)
    else:
        replay = replay_accounts(db)

    return {
        "status": "balanced" if not issues else "imbalanced",
        "ok": not issues,
        "issue_count": len(issues),
        "issues": issues,
        "backfill": backfill,
        "accounts": replay,
        "filters": {"company_id": company_id, "year": year},
    }


def serialize_event(event: LedgerEvent) -> dict:
    return {
        "id": event.id,
        "event_key": event.event_key,
        "trace_key": event.trace_key,
        "kind": event.kind,
        "domain": event.domain,
        "event_type": event.event_type,
        "status": event.status,
        "company_id": event.company_id,
        "year": event.year,
        "account_id": event.account_id,
        "transaction_id": event.transaction_id,
        "ref_type": event.ref_type,
        "ref_id": event.ref_id,
        "trade_order_id": event.trade_order_id,
        "auction_session_id": event.auction_session_id,
        "auction_bid_id": event.auction_bid_id,
        "auction_trade_id": event.auction_trade_id,
        "compliance_record_id": event.compliance_record_id,
        "report_id": event.report_id,
        "amount": float(event.amount or 0),
        "frozen_delta": float(event.frozen_delta or 0),
        "reserved_delta": float(event.reserved_delta or 0),
        "balance_after": float(event.balance_after or 0),
        "frozen_after": float(event.frozen_after or 0),
        "reserved_after": float(event.reserved_after or 0),
        "source": event.source,
        "detail": event.detail,
        "idempotency_key": event.idempotency_key,
        "occurred_at": event.occurred_at,
        "created_at": event.created_at,
    }

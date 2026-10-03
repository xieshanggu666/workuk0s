"""统一账本事件重放、旧账回填与对账测试。"""

from sqlalchemy import inspect

from app.models import AllowanceAccount, AllowanceTransaction, LedgerEvent
from app.services.ledger_replay import (
    backfill_legacy_ledger_events,
    reconcile_ledger,
    replay_accounts,
)
from app.services.quota_service import allocate_quota
from app.services.trade_order_service import confirm_order, create_order, deliver_order


def test_ledger_events_projected_and_replay_balances(db, seed):
    company = seed["company"]
    allocate_quota(db, company.id, 2026, baseline=100, allocation_amount=100)

    account = db.query(AllowanceAccount).filter_by(company_id=company.id, year=2026).one()
    events = db.query(LedgerEvent).filter_by(account_id=account.id).order_by(LedgerEvent.id).all()
    assert events
    allocation = next(e for e in events if e.event_type == "allocation")
    assert float(allocation.amount) == 100
    assert float(allocation.balance_after) == 100
    assert allocation.trace_key == f"company-year:{company.id}:2026"

    replayed = replay_accounts(db, company_id=company.id, year=2026)
    assert replayed[0]["replayed_current"] == 100
    assert replayed[0]["replayed_frozen"] == 0
    assert replayed[0]["replayed_reserved"] == 0

    result = reconcile_ledger(db, company_id=company.id, year=2026, auto_backfill=False)
    assert result["ok"] is True
    assert result["issue_count"] == 0


def test_legacy_transactions_and_opening_balance_are_backfilled(db, seed):
    from app.models import AllowanceAccount

    company = seed["company"]
    # 模拟旧库：先建表时已有 ledger_events，但业务数据来自旧版本、尚无事件投影。
    db.add(AllowanceAccount(
        company_id=company.id,
        year=2025,
        opening_balance=120,
        current_balance=120,
        frozen_balance=0,
        reserved_balance=0,
    ))
    db.commit()
    account = db.query(AllowanceAccount).filter_by(company_id=company.id, year=2025).one()
    db.add(AllowanceTransaction(
        account_id=account.id,
        company_id=company.id,
        tx_type="buy",
        amount=30,
        balance_after=150,
        frozen_after=0,
        reserved_after=0,
        tx_date="2025-01-01",
    ))
    db.commit()

    result = backfill_legacy_ledger_events(db)
    assert result["movement_events"] == 1
    assert result["legacy_opening_events"] == 1

    replayed = replay_accounts(db, company_id=company.id, year=2025)
    assert replayed[0]["replayed_current"] == 150
    assert replayed[0]["replay_seed"] == 0

    # 再跑一次不重复回填。
    again = backfill_legacy_ledger_events(db)
    assert again["inserted"] == 0
    assert reconcile_ledger(db, auto_backfill=False)["ok"] is True


def test_trade_order_lifecycle_has_traceable_state_events(db, seed):
    from app.models import Company

    seller = seed["company"]
    buyer = Company(code="B-001", name="买方企业", industry="电力", region="测试区")
    db.add(buyer)
    db.flush()
    allocate_quota(db, seller.id, 2026, baseline=100, allocation_amount=100)
    allocate_quota(db, buyer.id, 2026, baseline=100, allocation_amount=100)

    order = create_order(db, seller.id, buyer.id, 2026, 20, initiator="seller")
    confirm_order(db, order.id, buyer.id)
    deliver_order(db, order.id, seller.id)

    statuses = [
        (e.event_type, e.status)
        for e in db.query(LedgerEvent)
        .filter(LedgerEvent.trace_key == f"trade_order:{order.id}", LedgerEvent.kind == "state")
        .order_by(LedgerEvent.id)
        .all()
    ]
    assert ("order_status", "pending") in statuses
    assert ("order_status", "confirmed") in statuses
    assert ("order_status", "delivered") in statuses

    trace_movements = (
        db.query(LedgerEvent)
        .filter(LedgerEvent.trace_key == f"trade_order:{order.id}", LedgerEvent.kind == "movement")
        .count()
    )
    assert trace_movements >= 3
    assert reconcile_ledger(db, year=2026, auto_backfill=False)["ok"] is True


def test_ledger_event_table_exists(db):
    # Base.metadata 在测试 fixture 中已建表；这里确认统一事件模型已纳入元数据。
    assert "ledger_events" in inspect(db.bind).get_table_names()

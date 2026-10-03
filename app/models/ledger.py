from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    text,
)

from app.core.database import Base


class LedgerEvent(Base):
    """统一账本事件：配额流水、订单/竞价状态、履约与冲正回放的唯一投影。

    该表是权威业务表的只追加事件日志：
    - ``kind=movement`` 与 allowance_transactions 一一对应（旧流水由迁移补齐），
      携带带符号的持仓/冻结/占用增量及快照，可从零重放账户余额；
    - ``kind=state`` 记录企业订单、竞价场次/报价/成交、履约记录等领域状态迁移；
    - ``event_key`` 为确定性幂等键，并发重试或重复迁移只写入一次；
    - ``trace_key`` 把同一订单/成交/履约年度的资金事件和冲正补偿串在一起，
      支持部分冲正、违约追偿和跨年度隔离追踪。

    事件日志只追加、不覆盖；冲正通过追加反向/补偿事件表达，原始事件仍保留。
    """

    __tablename__ = "ledger_events"
    __table_args__ = (
        UniqueConstraint("event_key", name="uq_ledger_event_key"),
        # 一笔配额流水最多投影成一条 movement 事件；state 事件 transaction_id 为 NULL。
        Index(
            "uq_ledger_event_tx",
            "transaction_id",
            unique=True,
            sqlite_where=text("transaction_id IS NOT NULL"),
            postgresql_where=text("transaction_id IS NOT NULL"),
        ),
        Index("ix_ledger_company_year", "company_id", "year"),
        Index("ix_ledger_trace", "trace_key"),
        Index("ix_ledger_ref", "ref_type", "ref_id"),
    )

    id = Column(Integer, primary_key=True)
    event_key = Column(String(128), nullable=False)
    trace_key = Column(String(128), nullable=False, default="", index=True)

    # movement=配额账本事件；state=领域状态迁移事件（金额增量为 0）
    kind = Column(String(16), nullable=False, default="movement")
    # quota / trade_order / auction / compliance / reversal / legacy
    domain = Column(String(24), nullable=False, default="quota")
    event_type = Column(String(48), nullable=False)
    status = Column(String(24), nullable=False, default="posted")

    company_id = Column(Integer, ForeignKey("companies.id"), nullable=True, index=True)
    year = Column(Integer, nullable=True, index=True)
    account_id = Column(Integer, ForeignKey("allowance_accounts.id"), nullable=True, index=True)
    transaction_id = Column(
        Integer, ForeignKey("allowance_transactions.id"), nullable=True, index=True
    )

    # 通用业务对象引用；同时保留常用外键，便于直接查询与对账。
    ref_type = Column(String(32), nullable=False, default="")
    ref_id = Column(Integer, nullable=True)
    trade_order_id = Column(Integer, ForeignKey("trade_orders.id"), nullable=True, index=True)
    auction_session_id = Column(Integer, nullable=True, index=True)
    auction_bid_id = Column(Integer, nullable=True, index=True)
    auction_trade_id = Column(Integer, nullable=True, index=True)
    compliance_record_id = Column(Integer, nullable=True, index=True)
    report_id = Column(Integer, nullable=True, index=True)

    amount = Column(Numeric(18, 4), nullable=False, default=0)       # 带符号持仓增量
    frozen_delta = Column(Numeric(18, 4), nullable=False, default=0)
    reserved_delta = Column(Numeric(18, 4), nullable=False, default=0)
    balance_after = Column(Numeric(18, 4), nullable=False, default=0)
    frozen_after = Column(Numeric(18, 4), nullable=False, default=0)
    reserved_after = Column(Numeric(18, 4), nullable=False, default=0)

    source = Column(String(16), nullable=False, default="live")  # live/legacy/backfill
    detail = Column(String(500), nullable=False, default="")
    idempotency_key = Column(String(64), nullable=True, index=True)
    occurred_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

"""为旧库补齐统一账本事件表，并把配额流水、订单/竞价/履约终态回填为可重放事件。

用法：python scripts/migrate_ledger_replay.py（可重复执行）

- 新表：ledger_events（统一账本事件/状态轨迹，event_key 全局幂等）
- 旧流水：逐笔补 movement 事件，不修改原始 allowance_transactions
- 旧业务：补当前状态 state 事件，兼容迁移前无法完整恢复历史状态轨迹的旧记录
- 旧期初：对无法由 allocation 流水解释的 opening_balance 补 legacy_opening_balance
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import inspect

from app.core.database import Base, SessionLocal, engine  # noqa: E402
from app.models import LedgerEvent  # noqa: F401,E402
from app.services.ledger_replay import backfill_legacy_ledger_events  # noqa: E402


def main():
    before = set(inspect(engine).get_table_names())
    Base.metadata.create_all(engine)
    after = set(inspect(engine).get_table_names())
    new_tables = sorted(after - before)

    db = SessionLocal()
    try:
        result = backfill_legacy_ledger_events(db, commit=True)
    finally:
        db.close()

    if new_tables:
        print(f"新建表：{', '.join(new_tables)}")
    if result["inserted"]:
        print(
            "账本事件回填完成："
            f"{result['movement_events']} 条流水事件、"
            f"{result['state_events']} 条状态事件、"
            f"{result['legacy_opening_events']} 条旧期初事件"
        )
    else:
        print("无需迁移：统一账本事件表已存在且旧记录均已回填")


if __name__ == "__main__":
    main()

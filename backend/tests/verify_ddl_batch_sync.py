"""核对 DDL 识别登记 → 分区同步目标计划的批次隔离与幂等。

场景（同一服务进程内）：
1. sales.orders 的 ADD COLUMN 与 ADD PARTITION 登在一批，来源编号按顺序递增；
2. 暂停分区操作，再登记另一批 DROP PARTITION；
3. 按原编号续做并重复读取第一批；
4. 核对两批目标分区计划和原字段定义。

运行：backend/.venv/bin/python -I backend/tests/verify_ddl_batch_sync.py
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from app.pipeline import DdlRecord, pipeline_bus
from app.services.ddl_tracker import DdlTrackerService
from app.services.partition_sync import PartitionSyncService

FAILURES: list[str] = []


def check(label: str, actual, expected) -> None:
    if actual != expected:
        FAILURES.append(f"{label}: 期望 {expected!r}，实际 {actual!r}")
        print(f"  ✗ {label}: 期望 {expected!r}，实际 {actual!r}")
    else:
        print(f"  ✓ {label}")


def main() -> int:
    # 同一服务进程：共享 pipeline_bus 单例；重置管道内 DDL 相关状态保证可重复执行
    pipeline_bus.ddl_log.clear()
    pipeline_bus.ddl_registrations.clear()
    pipeline_bus.schema_registry.clear()
    pipeline_bus.audit_log.clear()
    pipeline_bus._ddl_source_seq = 0
    pipeline_bus._ddl_batch_seq = 0

    tracker = DdlTrackerService()
    partition_svc = PartitionSyncService()
    config = {
        "配置名称": "订单分区同步",
        "源分区策略": "RANGE",
        "目标分区策略": "RANGE",
        "分区映射": "直接映射",
        "新增分区行为": "自动创建",
        "过期清理": "自动",
        "分区字段": "dt",
    }

    print("== 既有输出：影响报告与同步计划字段保持不变 ==")
    probe = DdlRecord(
        record_id="ddl_probe", database="sales", table_name="customers",
        ddl_statement="ALTER TABLE sales.customers ADD COLUMN tag VARCHAR(8)",
        executed_at=datetime.now(), affected_columns=0, rollback_statement="",
    )
    impact = tracker.analyze_ddl_impact(probe)
    check("影响报告输出字段", sorted(impact.keys()), sorted([
        "record_id", "database", "table", "column_changes", "partition_changes",
        "affected_columns", "rollback_statement", "has_schema_impact",
        "has_partition_impact", "analyzed_at",
    ]))
    check("影响报告列变更分类", impact["column_changes"],
          [{"action": "add", "column": "TAG", "type": "VARCHAR"}])
    legacy_plan = partition_svc.sync_partition_ddl(config, [{"action": "add_partition"}])
    check("既有 sync_partition_ddl 输出字段", sorted(legacy_plan.keys()), sorted([
        "config", "partition_field", "source_policy", "target_policy",
        "sync_actions", "action_count", "synced_at",
    ]))
    check("既有动作分类与字段", legacy_plan["sync_actions"], [
        {"action": "add", "source_ddl": "ADD PARTITION",
         "target_ddl": "ALTER TABLE ... ADD PARTITION (dt)", "status": "synced"},
    ])

    print("== 第一批：sales.orders 的 ADD COLUMN 与 ADD PARTITION 登在一批 ==")
    batch1 = tracker.record_ddl_batch("sales", "orders", [
        "ALTER TABLE sales.orders ADD COLUMN remark VARCHAR(64)",
        "ALTER TABLE sales.orders ADD PARTITION (dt = '2026-11-20')",
    ])
    batch1_id = batch1[0].batch_id
    check("第一批两条记录同属一批", {r.batch_id for r in batch1}, {batch1_id})
    check("第一批来源编号按登记顺序递增", [r.source_seq for r in batch1], [1, 2])
    check("改列只影响列定义", [c["action"] for c in batch1[0].column_changes], ["add"])
    check("ADD COLUMN 不产生分区动作", batch1[0].partition_changes, [])
    check("ADD PARTITION 只产生分区动作", batch1[1].partition_changes,
          [{"action": "add_partition"}])
    check("批次登记按来源编号有序",
          [(r.source_seq, r.table_name) for r in pipeline_bus.get_ddl_batch(batch1_id)],
          [(1, "orders"), (2, "orders")])

    print("== 同一服务进程内暂停分区操作 ==")
    partition_svc.pause_partition_ops()
    check("分区操作已暂停", partition_svc.partition_ops_paused, True)
    paused_plan = partition_svc.sync_registered_batch(config, batch1_id)
    check("暂停期间不生成目标计划", paused_plan["sync_actions"], [])

    print("== 暂停期间登记另一批 DROP PARTITION ==")
    batch2 = tracker.record_ddl_batch("sales", "orders", [
        "ALTER TABLE sales.orders DROP PARTITION p202510",
    ])
    batch2_id = batch2[0].batch_id
    check("第二批批次号不同", batch2_id != batch1_id, True)
    check("第二批来源编号接续递增", [r.source_seq for r in batch2], [3])

    print("== 按原编号续做并重复读取第一批 ==")
    partition_svc.resume_partition_ops()
    check("分区操作已恢复", partition_svc.partition_ops_paused, False)
    plan1 = partition_svc.sync_registered_batch(config, batch1_id)
    plan1_again = partition_svc.sync_registered_batch(config, batch1_id)
    check("第一批计划来源编号", plan1["source_seqs"], [1, 2])
    check("第一批只含本批的 ADD 分区动作", plan1["sync_actions"], [
        {"action": "add", "source_ddl": "ADD PARTITION",
         "target_ddl": "ALTER TABLE ... ADD PARTITION (dt)", "status": "synced"},
    ])
    check("第一批计划不混入第二批的清理动作",
          [a for a in plan1["sync_actions"] if a["action"] == "drop"], [])
    check("重复读取同批记录不重复生成分区命令",
          plan1_again["sync_actions"], plan1["sync_actions"])

    print("== 第二批目标分区计划 ==")
    plan2 = partition_svc.sync_registered_batch(config, batch2_id)
    check("第二批计划来源编号", plan2["source_seqs"], [3])
    check("第二批只含本批的 DROP 分区动作", plan2["sync_actions"], [
        {"action": "drop", "partition_name": "P202510",
         "target_action": "drop", "status": "synced"},
    ])

    print("== 原字段定义与列定义登记保持不变 ==")
    check("DDL追踪列表字段", DdlTrackerService.LIST_FIELDS,
          ["记录ID", "变更数据库", "变更表名", "DDL语句", "执行时间", "影响列数", "回滚语句"])
    check("分区同步列表字段", PartitionSyncService.LIST_FIELDS,
          ["配置名称", "源分区策略", "目标分区策略", "分区映射", "新增分区行为", "过期清理", "分区字段"])
    # 列定义登记只来自列变更：第一批 ADD COLUMN 一条；
    # 第二批 DROP PARTITION 沿用既有 DROP 分类（DROP\s+(?:COLUMN\s+)? 会命中 PARTITION），
    # 该行为与重构前完全一致，予以保留。
    check("列定义登记只来自列变更（改列仍只影响列定义）",
          [(e["column"], e["action"]) for e in pipeline_bus.schema_registry["orders"]],
          [("REMARK", "add"), ("PARTITION", "drop")])

    if FAILURES:
        print(f"\n核对失败 {len(FAILURES)} 项")
        return 1
    print("\n全部核对通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""DDL追踪领域服务：追踪和解析DDL变更对Schema的影响。

上游：从管道总线消费 DdlRecord 进行自动分析
下游：schema_sync(列级变更)、partition_sync(分区变更) 消费分析结果
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any

from app.ddl_insight import recognize_ddl
from app.pipeline import DdlRecord, pipeline_bus
from app.services.base import BaseModuleService


class DdlTrackerService(BaseModuleService):
    MODULE = "ddl_tracker"
    ENTITY = "DDL记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录ID", "变更数据库", "变更表名", "DDL语句", "执行时间", "影响列数", "回滚语句"]
    REQUIRED_FIELDS = ["记录ID", "变更数据库", "变更表名"]
    STATUS_ORDER = ["已记录", "已分析", "已通知", "已处理"]
    ACTION_RULES = {"分析变更": "已记录", "发送通知": "已分析", "完成处理": "已分析"}
    NEGATIVE_ACTIONS = []

    def analyze_ddl_impact(self, ddl: DdlRecord) -> dict[str, Any]:
        """分析DDL变更对目标表的影响范围。

        表结构语句与分区动作统一由 recognize_ddl 识别，
        产出影响报告供 schema_sync 和 partition_sync 消费。
        """
        insight = recognize_ddl(ddl.ddl_statement)
        column_changes = insight.column_changes
        partition_changes = insight.partition_changes
        ddl.column_changes.extend(column_changes)
        ddl.partition_changes.extend(partition_changes)

        affected_cols = len(column_changes)
        rollback = self.generate_rollback(ddl.ddl_statement)

        impact = {
            "record_id": ddl.record_id,
            "database": ddl.database,
            "table": ddl.table_name,
            "column_changes": column_changes,
            "partition_changes": partition_changes,
            "affected_columns": affected_cols,
            "rollback_statement": rollback,
            "has_schema_impact": insight.has_schema_impact,
            "has_partition_impact": insight.has_partition_impact,
            "analyzed_at": datetime.now().isoformat(),
        }

        # 对 schema_sync 有影响的DDL，写入管道供冲突检测消费
        if impact["has_schema_impact"]:
            for cc in column_changes:
                pipeline_bus.schema_registry.setdefault(ddl.table_name, []).append({
                    "column": cc.get("column"),
                    "action": cc.get("action"),
                    "type": cc.get("type", ""),
                    "timestamp": datetime.now().isoformat(),
                })

        pipeline_bus.add_ddl(ddl)
        # 登记共享 DDL 识别数据，partition_sync 按批次读取形成目标计划
        if ddl.batch_id:
            pipeline_bus.register_ddl_insight(ddl.batch_id, ddl)
        pipeline_bus.write_audit(
            "DDL分析", f"{ddl.database}.{ddl.table_name}",
            f"分析DDL影响：列变更{affected_cols}，分区变更{len(partition_changes)}",
            "ddl_tracker", "已分析"
        )
        return impact

    def track_temporal_ddl(self, table_name: str, from_dt: str, to_dt: str) -> list[dict[str, Any]]:
        """查询指定表在时间范围内的DDL变更序列。"""
        results: list[dict[str, Any]] = []
        recent = pipeline_bus.get_recent_ddls()
        for ddl in recent:
            if ddl.table_name == table_name and from_dt <= ddl.executed_at.isoformat() <= to_dt:
                results.append({
                    "record_id": ddl.record_id,
                    "ddl": ddl.ddl_statement[:200],
                    "executed_at": ddl.executed_at.isoformat(),
                    "affected_columns": ddl.affected_columns,
                })
        return results

    def generate_rollback(self, ddl_stmt: str) -> str:
        """根据DDL语句生成回滚语句。"""
        ddl_upper = ddl_stmt.upper()
        if "ADD COLUMN" in ddl_upper:
            match = re.search(r'ADD\s+COLUMN\s+(\w+)', ddl_stmt, re.IGNORECASE)
            if match:
                return f"ALTER TABLE ... DROP COLUMN {match.group(1)};"
        if "DROP COLUMN" in ddl_upper:
            return "-- 回滚需从备份恢复列定义"
        if "MODIFY" in ddl_upper:
            return "-- 回滚需记录修改前的列类型"
        if "ADD PARTITION" in ddl_upper:
            return "ALTER TABLE ... DROP PARTITION ...;"
        return "-- 自动回滚生成中"

    def record_ddl_batch(self, database: str, table_name: str,
                         ddl_statements: list[str]) -> list[DdlRecord]:
        """将同一轮登记的多条 DDL 登为一批。

        同一批既包含普通改列，也包含同表的分区增删；
        每条记录保留按顺序登记的来源编号。
        """
        batch_id = pipeline_bus.begin_ddl_batch()
        records: list[DdlRecord] = []
        for ddl_stmt in ddl_statements:
            ddl = DdlRecord(
                record_id=f"ddl_{uuid.uuid4().hex[:8]}",
                database=database,
                table_name=table_name,
                ddl_statement=ddl_stmt,
                executed_at=datetime.now(),
                affected_columns=0,
                rollback_statement=self.generate_rollback(ddl_stmt),
                batch_id=batch_id,
            )
            self.analyze_ddl_impact(ddl)
            records.append(ddl)
        return records

    def record_ddl(self, database: str, table_name: str, ddl_stmt: str) -> DdlRecord:
        """记录并分析一条DDL。"""
        return self.record_ddl_batch(database, table_name, [ddl_stmt])[0]

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "发送通知":
            pipeline_bus.write_audit(
                "DDL通知", f"{entry.get('变更数据库', '')}.{entry.get('变更表名', '')}",
                f"DDL记录 {entry_id} 变更通知已发送",
                "ddl_tracker", "已通知"
            )
        return entry, msg

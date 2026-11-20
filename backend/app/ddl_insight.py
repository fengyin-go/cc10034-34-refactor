"""DDL 识别数据：表结构语句与分区动作的统一解析结果。

ddl_tracker 与 partition_sync 两处共用本模块的分类判断：
- ddl_tracker 解析并登记识别数据（按批次 + 来源编号）；
- partition_sync 消费识别数据形成目标分区计划。

分类判断只在这里写一遍，两处不再各自维护动作词表。
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

# 列变更动作分类
COLUMN_ACTION_ADD = "add"
COLUMN_ACTION_DROP = "drop"
COLUMN_ACTION_MODIFY = "modify"

# 分区变更动作分类
PARTITION_ACTION_ADD = "add_partition"
PARTITION_ACTION_DROP = "drop_partition"


@dataclass
class DdlInsight:
    """DDL 识别数据：一条 DDL 语句解析出的列变更与分区变更。"""

    column_changes: list[dict[str, Any]] = field(default_factory=list)
    partition_changes: list[dict[str, Any]] = field(default_factory=list)

    @property
    def affected_columns(self) -> int:
        """受影响列数。"""
        return len(self.column_changes)

    @property
    def has_schema_impact(self) -> bool:
        """是否影响表结构（列定义）。"""
        return len(self.column_changes) > 0

    @property
    def has_partition_impact(self) -> bool:
        """是否影响分区。"""
        return len(self.partition_changes) > 0


def recognize_ddl(ddl_statement: str) -> DdlInsight:
    """解析 DDL 语句，产出 DDL 识别数据（列变更 + 分区变更）。

    表结构解析与分区解析的唯一入口，分类判断只在这里写一遍。
    """
    ddl_text = ddl_statement.upper()
    insight = DdlInsight()

    # 检测 ADD COLUMN
    add_cols = re.findall(r'ADD\s+COLUMN\s+(\w+)\s+(\w+)', ddl_text, re.IGNORECASE)
    for col_name, col_type in add_cols:
        insight.column_changes.append({
            "action": COLUMN_ACTION_ADD, "column": col_name, "type": col_type,
        })

    # 检测 DROP COLUMN
    drop_cols = re.findall(r'DROP\s+(?:COLUMN\s+)?(\w+)', ddl_text, re.IGNORECASE)
    for col_name in drop_cols:
        insight.column_changes.append({"action": COLUMN_ACTION_DROP, "column": col_name})

    # 检测 MODIFY COLUMN / CHANGE COLUMN
    modify_cols = re.findall(r'(?:MODIFY|CHANGE)\s+(?:COLUMN\s+)?(\w+)\s+(\w+)\s+(\w+)',
                             ddl_text, re.IGNORECASE)
    for old_name, new_name, col_type in modify_cols:
        insight.column_changes.append({
            "action": COLUMN_ACTION_MODIFY, "column": old_name, "new_type": col_type,
        })

    # 检测 ADD PARTITION
    add_part = re.findall(r'ADD\s+PARTITION', ddl_text, re.IGNORECASE)
    if add_part:
        insight.partition_changes.append({"action": PARTITION_ACTION_ADD})

    # 检测 DROP PARTITION
    drop_part = re.findall(r'DROP\s+PARTITION\s+(\w+)', ddl_text, re.IGNORECASE)
    for part_name in drop_part:
        insight.partition_changes.append({
            "action": PARTITION_ACTION_DROP, "name": part_name,
        })

    return insight

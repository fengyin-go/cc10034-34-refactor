"""分区同步领域服务：管理分区表的DDL同步策略。

上游：ddl_tracker 检测分区变更 → partition_sync 消费
消费：schema_sync 消费分区映射进行列信息同步
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.ddl_insight import PARTITION_ACTION_ADD, PARTITION_ACTION_DROP
from app.pipeline import pipeline_bus
from app.services.base import BaseModuleService


class PartitionSyncService(BaseModuleService):
    MODULE = "partition_sync"
    ENTITY = "同步配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["配置名称", "源分区策略", "目标分区策略", "分区映射", "新增分区行为", "过期清理", "分区字段"]
    REQUIRED_FIELDS = ["配置名称", "源分区策略", "目标分区策略"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用配置": "草稿", "停用配置": "已启用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    def __init__(self) -> None:
        super().__init__()
        # 同一服务进程内的分区操作暂停标记
        self._partition_ops_paused = False

    def pause_partition_ops(self) -> None:
        """暂停分区操作（同一服务进程内生效）。"""
        self._partition_ops_paused = True

    def resume_partition_ops(self) -> None:
        """恢复分区操作，按原编号续做。"""
        self._partition_ops_paused = False

    @property
    def partition_ops_paused(self) -> bool:
        """分区操作是否已暂停。"""
        return self._partition_ops_paused

    def sync_partition_ddl(self, config: dict[str, Any], ddl_partition_changes: list[dict[str, Any]]) -> dict[str, Any]:
        """根据DDL中检测到的分区变更生成目标DDL。

        从 ddl_tracker 产出的 partition_changes 读取变更列表，
        结合分区映射和增量清理策略生成同步DDL。
        """
        source_policy = config.get("源分区策略", "RANGE")
        target_policy = config.get("目标分区策略", "RANGE")
        partition_mapping = config.get("分区映射", "直接映射")
        add_behavior = config.get("新增分区行为", "自动创建")
        expire_cleanup = config.get("过期清理", "自动") == "自动"
        partition_field = config.get("分区字段", "dt")

        sync_actions: list[dict[str, Any]] = []
        for change in ddl_partition_changes:
            sync_action = self._classify_partition_change(
                change,
                target_policy=target_policy,
                partition_field=partition_field,
                add_behavior=add_behavior,
                expire_cleanup=expire_cleanup,
            )
            if sync_action is not None:
                sync_actions.append(sync_action)

        pipeline_bus.write_audit(
            "分区同步", partition_field,
            f"同步 {len(sync_actions)} 个分区变更操作",
            "partition_sync", "已同步"
        )
        return {
            "config": config.get("配置名称"),
            "partition_field": partition_field,
            "source_policy": source_policy,
            "target_policy": target_policy,
            "sync_actions": sync_actions,
            "action_count": len(sync_actions),
            "synced_at": datetime.now().isoformat(),
        }

    def sync_registered_batch(self, config: dict[str, Any], batch_id: str) -> dict[str, Any]:
        """按批次读取 DDL 识别登记，形成目标分区计划。

        同一次同步的所有分区动作来自同一批次的登记：
        旧批恢复时不会混入其他批次后来登记的同表清理动作；
        重复读取同批记录只生成一次分区命令。
        """
        if self._partition_ops_paused:
            return {
                "config": config.get("配置名称"),
                "batch_id": batch_id,
                "status": "已暂停",
                "sync_actions": [],
                "action_count": 0,
            }
        registrations = pipeline_bus.get_ddl_batch(batch_id)
        partition_changes: list[dict[str, Any]] = []
        for registration in registrations:
            partition_changes.extend(registration.partition_changes)
        plan = self.sync_partition_ddl(config, partition_changes)
        plan["batch_id"] = batch_id
        plan["source_seqs"] = [registration.source_seq for registration in registrations]
        return plan

    def _classify_partition_change(self, change: dict[str, Any], *,
                                   target_policy: str, partition_field: str,
                                   add_behavior: str, expire_cleanup: bool) -> dict[str, Any] | None:
        """分区动作分类：add_partition/drop_partition → 目标同步动作。

        动作词表与 ddl_tracker 共用 ddl_insight 中的分类常量，
        既有动作分类、列字段和分区执行顺序保持不变。
        """
        action = change.get("action", "")
        if action == PARTITION_ACTION_ADD:
            return {
                "action": "add",
                "source_ddl": "ADD PARTITION",
                "target_ddl": self._build_target_add_partition(
                    target_policy, partition_field, add_behavior),
                "status": "synced",
            }
        if action == PARTITION_ACTION_DROP:
            cleanup_action = "drop" if expire_cleanup else "archive"
            return {
                "action": "drop",
                "partition_name": change.get("name", ""),
                "target_action": cleanup_action,
                "status": "synced",
            }
        return None

    def _build_target_add_partition(self, target_policy: str, field: str, behavior: str) -> str:
        if behavior == "自动创建":
            return f"ALTER TABLE ... ADD PARTITION ({field})"
        return f"-- 待手动创建分区 ({field})，策略：{target_policy}"

    def detect_partition_changes(self, source_table: str, target_table: str) -> dict[str, Any]:
        """检测分区表的变化差异。"""
        recent_ddls = pipeline_bus.get_recent_ddls(limit=100)
        partition_ddls = []
        for ddl in recent_ddls:
            if ddl.table_name == source_table and ddl.partition_changes:
                partition_ddls.append({
                    "ddl_id": ddl.record_id,
                    "changes": ddl.partition_changes,
                    "executed_at": ddl.executed_at.isoformat(),
                })
        return {
            "source_table": source_table,
            "target_table": target_table,
            "partition_ddl_count": len(partition_ddls),
            "ddls": partition_ddls,
        }

    def estimate_partition_growth(self, field: str, policy: str) -> dict[str, Any]:
        """估算分区增长趋势。"""
        partitions_per_month = 30 if policy in ("RANGE", "LIST") else 1
        retention_days = 90 if policy == "RANGE" else 30
        return {
            "partition_field": field,
            "policy": policy,
            "partitions_per_month_estimate": partitions_per_month,
            "retention_days": retention_days,
            "monthly_storage_factor": partitions_per_month * 10,  # ~10GB/分区
            "recommended_backup_window": "7d",
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "启用配置":
            field = entry.get("分区字段", "dt")
            pipeline_bus.write_audit(
                "分区策略启用", field,
                f"分区同步配置 {entry.get('配置名称')} 已启用",
                "partition_sync", "已启用"
            )
        return entry, msg

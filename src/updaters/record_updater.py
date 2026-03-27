"""
记录更新器。

职责：根据 record id 找到已有记录，更新指定字段后写回。
不创建新记录，只更新已存在的记录。

典型使用场景：
  1. PDF 文本提取完成后，回填 content_text / content_status / text_file_path
  2. AI 摘要生成后，回填 summary / tags
  3. 手动修正某条记录的 category / title
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from models.record import ContentStatus, Record
from storage.jsonl_store import JsonlStore
from storage.path_builder import PathBuilder


@dataclass
class UpdatePayload:
    """
    描述要更新哪些字段。只有非 None 的字段会被写入。
    用 dataclass 而非 dict，避免拼错字段名。
    """
    content_text   : str | None = None
    content_status : str | None = None
    text_file_path : str | None = None
    raw_file_path  : str | None = None
    content_hash   : str | None = None   # 不传时若 content_text 有值则自动计算
    summary        : str | None = None
    tags           : list[str] | None = None


class RecordUpdater:
    def __init__(self, store: JsonlStore | None = None):
        self._store = store or JsonlStore()

    def update_by_id(
        self,
        record_id: str,
        publish_date: str,
        payload: UpdatePayload,
    ) -> bool:
        """
        按 id 更新单条记录。

        参数：
            record_id    — 目标记录的 id
            publish_date — 记录所在的 JSONL 文件日期（用于定位文件）
            payload      — 要更新的字段

        返回：
            True  — 找到并更新成功
            False — 未找到该 id
        """
        path = self._store._pb.record_file(publish_date)
        records = {r.id: r for r in self._store._iter_file(path)}

        if record_id not in records:
            return False

        self._apply(records[record_id], payload)
        self._store._rewrite(path, list(records.values()))
        return True

    def update_content(
        self,
        record_id: str,
        publish_date: str,
        content_text: str,
        text_file_path: str = "",
        status: str = ContentStatus.CLEANED,
    ) -> bool:
        """
        便捷方法：回填正文内容（PDF 提取或 HTML 清洗后调用）。
        自动计算 content_hash。
        """
        payload = UpdatePayload(
            content_text=content_text,
            content_status=status,
            text_file_path=text_file_path,
            content_hash=self._hash(content_text),
        )
        return self.update_by_id(record_id, publish_date, payload)

    def update_summary(
        self,
        record_id: str,
        publish_date: str,
        summary: str,
        tags: list[str] | None = None,
    ) -> bool:
        """
        便捷方法：回填 AI 摘要和标签。
        """
        payload = UpdatePayload(
            summary=summary,
            tags=tags or [],
            content_status=ContentStatus.SUMMARIZED,
        )
        return self.update_by_id(record_id, publish_date, payload)

    # ── 内部 ──────────────────────────────────────────────────────────

    @staticmethod
    def _apply(record: Record, payload: UpdatePayload) -> None:
        """将 payload 中非 None 的字段写入 record（原地修改）。"""
        if payload.content_text is not None:
            record.content_text = payload.content_text
            # 有 content_text 时自动补 hash（除非 payload 显式给了 hash）
            if payload.content_hash is None:
                record.content_hash = RecordUpdater._hash(payload.content_text)
        if payload.content_status  is not None:
            record.content_status = payload.content_status
        if payload.text_file_path  is not None:
            record.text_file_path = payload.text_file_path
        if payload.raw_file_path   is not None:
            record.raw_file_path  = payload.raw_file_path
        if payload.content_hash    is not None:
            record.content_hash   = payload.content_hash
        if payload.summary         is not None:
            record.summary        = payload.summary
        if payload.tags            is not None:
            record.tags           = payload.tags

    @staticmethod
    def _hash(text: str) -> str:
        return hashlib.sha256(text.encode()).hexdigest()[:16]

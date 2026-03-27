"""
记录读取器。

所有过滤条件均可选，支持组合使用。
底层数据来自 JsonlStore，过滤在内存中完成。

使用示例：
    reader = RecordReader()

    # 读取某天全部记录
    records = reader.by_date("2026-03-27")

    # 按日期范围 + 来源过滤
    records = reader.by_date_range("2026-03-01", "2026-03-27").source("bseinfo").fetch()

    # 按公司 + 分类过滤
    records = reader.by_date("2026-03-27").company_code("920445").fetch()
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from storage.jsonl_store import JsonlStore

if TYPE_CHECKING:
    from models.record import Record


class _Query:
    """
    惰性查询构建器。
    调用 .fetch() 才真正执行加载和过滤。
    """

    def __init__(self, store: JsonlStore, date: str | None, start: str | None, end: str | None):
        self._store  = store
        self._date   = date
        self._start  = start
        self._end    = end

        self._source         : str | None = None
        self._source_type    : str | None = None
        self._record_type    : str | None = None
        self._company_code   : str | None = None
        self._company_name   : str | None = None
        self._category       : str | None = None
        self._content_status : str | None = None

    # ── 过滤条件设置（返回 self 支持链式调用）────────────────────────

    def source(self, value: str) -> _Query:
        self._source = value
        return self

    def source_type(self, value: str) -> _Query:
        self._source_type = value
        return self

    def record_type(self, value: str) -> _Query:
        self._record_type = value
        return self

    def company_code(self, value: str) -> _Query:
        self._company_code = value
        return self

    def company_name(self, value: str) -> _Query:
        """支持模糊匹配（包含即命中）。"""
        self._company_name = value
        return self

    def category(self, value: str) -> _Query:
        self._category = value
        return self

    def content_status(self, value: str) -> _Query:
        self._content_status = value
        return self

    # ── 执行查询 ──────────────────────────────────────────────────────

    def fetch(self) -> list[Record]:
        if self._date:
            records = self._store.read_by_date(self._date)
        else:
            records = self._store.read_by_date_range(self._start, self._end)

        return [r for r in records if self._match(r)]

    def _match(self, r: Record) -> bool:
        if self._source         and r.source         != self._source:
            return False
        if self._source_type    and r.source_type    != self._source_type:
            return False
        if self._record_type    and r.record_type    != self._record_type:
            return False
        if self._company_code   and r.company_code   != self._company_code:
            return False
        if self._company_name   and self._company_name not in r.company_name:
            return False
        if self._category       and r.category       != self._category:
            return False
        if self._content_status and r.content_status != self._content_status:
            return False
        return True


class RecordReader:
    """对外暴露的读取入口。"""

    def __init__(self, store: JsonlStore | None = None):
        self._store = store or JsonlStore()

    def by_date(self, date_str: str) -> _Query:
        """读取某一天的记录，返回可继续过滤的 Query。"""
        return _Query(self._store, date=date_str, start=None, end=None)

    def by_date_range(self, start: str, end: str) -> _Query:
        """读取日期范围内的记录，返回可继续过滤的 Query。"""
        return _Query(self._store, date=None, start=start, end=end)

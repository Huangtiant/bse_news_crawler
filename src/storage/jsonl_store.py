"""
JSONL 存储层。

职责：向 data/records/{date}.jsonl 读写记录。

写入有两种模式：
  append_record / append_records  — 无去重，直接追加（适合首次全量写入）
  upsert_record / upsert_records  — 幂等写入，id 已存在则合并更新，不存在则追加
"""
from __future__ import annotations

from datetime import date as Date
from pathlib import Path
from typing import Iterator

from models.record import Record
from storage.path_builder import PathBuilder


class JsonlStore:
    def __init__(self, path_builder: PathBuilder | None = None):
        self._pb = path_builder or PathBuilder()

    # ── 写入 ──────────────────────────────────────────────────────────

    def append_record(self, record: Record) -> None:
        """追加单条记录到对应日期的 JSONL 文件。"""
        path = self._pb.record_file(record.publish_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(record.to_json_line() + "\n")

    def append_records(self, records: list[Record]) -> None:
        """批量追加，按 publish_date 分组后逐文件写入，减少 open 次数。"""
        grouped: dict[str, list[Record]] = {}
        for r in records:
            grouped.setdefault(r.publish_date, []).append(r)

        for date_str, group in grouped.items():
            path = self._pb.record_file(date_str)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as f:
                for r in group:
                    f.write(r.to_json_line() + "\n")

    def upsert_record(self, record: Record) -> bool:
        """
        幂等写入单条记录。

        - id 不存在：追加一行，返回 True（新增）
        - id 已存在：用可更新字段合并后重写整文件，返回 False（更新）

        可更新字段（首次写入后允许覆盖）：
            fetch_time, raw_file_path, text_file_path,
            content_text, content_status, content_hash,
            summary, tags
        不可更新字段（首次写入后保持不变）：
            id, source, source_type, record_type, publish_date,
            url, pdf_url, company_code, company_name, title,
            category, language, extra
        """
        path = self._pb.record_file(record.publish_date)
        existing = {r.id: r for r in self._iter_file(path)}

        if record.id not in existing:
            # 新记录：直接追加
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as f:
                f.write(record.to_json_line() + "\n")
            return True

        # 已存在：合并可更新字段后重写
        existing[record.id] = self._merge(existing[record.id], record)
        self._rewrite(path, list(existing.values()))
        return False

    def upsert_records(self, records: list[Record]) -> dict[str, int]:
        """
        批量幂等写入，按 publish_date 分组处理。

        返回 {"inserted": N, "updated": M}。
        """
        grouped: dict[str, list[Record]] = {}
        for r in records:
            grouped.setdefault(r.publish_date, []).append(r)

        inserted = updated = 0
        for date_str, group in grouped.items():
            path = self._pb.record_file(date_str)
            existing = {r.id: r for r in self._iter_file(path)}

            new_rows: list[Record] = []
            for r in group:
                if r.id not in existing:
                    existing[r.id] = r
                    inserted += 1
                    new_rows.append(r)
                else:
                    existing[r.id] = self._merge(existing[r.id], r)
                    updated += 1

            if updated > 0:
                # 有更新时重写整文件
                self._rewrite(path, list(existing.values()))
            elif new_rows:
                # 纯新增时只追加，效率更高
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("a", encoding="utf-8") as f:
                    for r in new_rows:
                        f.write(r.to_json_line() + "\n")

        return {"inserted": inserted, "updated": updated}

    # ── 读取 ──────────────────────────────────────────────────────────

    def read_by_date(self, date_str: str) -> list[Record]:
        """读取某一天全部记录。文件不存在时返回空列表。"""
        path = self._pb.record_file(date_str)
        return list(self._iter_file(path))

    def read_by_date_range(self, start: str, end: str) -> list[Record]:
        """
        读取日期范围内的全部记录（含两端）。
        start / end 格式均为 "YYYY-MM-DD"。
        """
        start_d = Date.fromisoformat(start)
        end_d   = Date.fromisoformat(end)

        records_dir = self._pb.record_file("placeholder").parent
        if not records_dir.exists():
            return []

        results: list[Record] = []
        for jsonl_file in sorted(records_dir.glob("*.jsonl")):
            file_date = Date.fromisoformat(jsonl_file.stem)
            if start_d <= file_date <= end_d:
                results.extend(self._iter_file(jsonl_file))
        return results

    # ── 内部 ─────────────────────────────────────────────────────────

    @staticmethod
    def _merge(base: Record, update: Record) -> Record:
        """用 update 的可更新字段覆盖 base，不可更新字段保持 base 原值。"""
        base.fetch_time      = update.fetch_time
        base.raw_file_path   = update.raw_file_path   or base.raw_file_path
        base.text_file_path  = update.text_file_path  or base.text_file_path
        base.content_text    = update.content_text    or base.content_text
        base.content_status  = update.content_status  or base.content_status
        base.content_hash    = update.content_hash    or base.content_hash
        base.summary         = update.summary         or base.summary
        base.tags            = update.tags            if update.tags else base.tags
        return base

    @staticmethod
    def _rewrite(path: Path, records: list[Record]) -> None:
        """将记录列表原子性地重写到文件（先写临时文件再替换）。"""
        tmp = path.with_suffix(".tmp")
        path.parent.mkdir(parents=True, exist_ok=True)
        with tmp.open("w", encoding="utf-8") as f:
            for r in records:
                f.write(r.to_json_line() + "\n")
        tmp.replace(path)

    @staticmethod
    def _iter_file(path: Path) -> Iterator[Record]:
        """逐行解析 JSONL 文件，跳过空行和损坏行。"""
        if not path.exists():
            return
        import json
        with path.open(encoding="utf-8") as f:
            for lineno, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield Record.from_dict(json.loads(line))
                except Exception as e:
                    print(f"[jsonl_store] 跳过损坏行 {path}:{lineno} — {e}")

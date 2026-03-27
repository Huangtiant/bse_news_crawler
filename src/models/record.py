"""
统一记录模型。

所有来源（北交所公告、东方财富文章等）均映射到 Record 后再落盘，
保证存储层始终面对同一种结构。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


# ---------- content_status 可选值 ----------
class ContentStatus:
    EMPTY = "empty"          # 仅有元数据，无正文
    DOWNLOADED = "downloaded"  # 原始文件（PDF/HTML）已下载到本地
    EXTRACTED = "extracted"  # 文本已从原始文件提取
    CLEANED = "cleaned"      # 文本已清洗归一化
    SUMMARIZED = "summarized"  # AI 摘要已生成
    FAILED = "failed"        # 处理过程中出现错误


# ---------- 主记录结构 ----------
@dataclass
class Record:
    # --- 身份 ---
    id: str = ""                  # 稳定 hash，见 make_id()
    source: str = ""              # "bseinfo" | "eastmoney" | ...
    source_type: str = ""         # "official"（交易所）| "media"（财经媒体）
    record_type: str = ""         # "announcement" | "article" | "report"

    # --- 公司 ---
    company_code: str = ""        # 股票代码，如 "920445"；来源无此信息时留空
    company_name: str = ""        # 公司简称

    # --- 内容元数据 ---
    title: str = ""
    category: str = ""            # 公告分类，如 "年度报告"；文章来源可留空或填 "研报"
    publish_date: str = ""        # YYYY-MM-DD，按此字段分文件存储
    fetch_time: str = ""          # ISO 8601，记录本条数据何时被抓取

    # --- 链接 ---
    url: str = ""                 # 网页地址
    pdf_url: str = ""             # PDF 直链，无则留空

    # --- 本地文件 ---
    raw_file_path: str = ""       # 原始文件相对路径（PDF 或 HTML）
    text_file_path: str = ""      # 清洗后 txt 相对路径

    # --- 正文 ---
    content_text: str = ""        # 提取/清洗后的正文文本，初始为空
    content_status: str = ContentStatus.EMPTY
    content_hash: str = ""        # sha256(content_text)，用于变更检测

    # --- 后处理 ---
    summary: str = ""             # AI 生成的摘要
    language: str = "zh"
    tags: list[str] = field(default_factory=list)

    # --- 来源特有字段 ---
    extra: dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # 工厂方法
    # ------------------------------------------------------------------

    @staticmethod
    def make_id(source: str, unique_key: str) -> str:
        """
        生成稳定 id。

        规则：sha256("{source}:{unique_key}") 取前 16 位。
        - bseinfo:  unique_key = pdf_url
        - eastmoney: unique_key = infocode
        """
        raw = f"{source}:{unique_key}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ------------------------------------------------------------------
    # 序列化
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "source": self.source,
            "source_type": self.source_type,
            "record_type": self.record_type,
            "company_code": self.company_code,
            "company_name": self.company_name,
            "title": self.title,
            "category": self.category,
            "publish_date": self.publish_date,
            "fetch_time": self.fetch_time,
            "url": self.url,
            "pdf_url": self.pdf_url,
            "raw_file_path": self.raw_file_path,
            "text_file_path": self.text_file_path,
            "content_text": self.content_text,
            "content_status": self.content_status,
            "content_hash": self.content_hash,
            "summary": self.summary,
            "language": self.language,
            "tags": self.tags,
            "extra": self.extra,
        }

    @classmethod
    def from_dict(cls, d: dict) -> Record:
        return cls(
            id=d.get("id", ""),
            source=d.get("source", ""),
            source_type=d.get("source_type", ""),
            record_type=d.get("record_type", ""),
            company_code=d.get("company_code", ""),
            company_name=d.get("company_name", ""),
            title=d.get("title", ""),
            category=d.get("category", ""),
            publish_date=d.get("publish_date", ""),
            fetch_time=d.get("fetch_time", ""),
            url=d.get("url", ""),
            pdf_url=d.get("pdf_url", ""),
            raw_file_path=d.get("raw_file_path", ""),
            text_file_path=d.get("text_file_path", ""),
            content_text=d.get("content_text", ""),
            content_status=d.get("content_status", ContentStatus.EMPTY),
            content_hash=d.get("content_hash", ""),
            summary=d.get("summary", ""),
            language=d.get("language", "zh"),
            tags=d.get("tags", []),
            extra=d.get("extra", {}),
        )

    def to_json_line(self) -> str:
        """序列化为单行 JSON（不带换行符），用于 JSONL 写入。"""
        return json.dumps(self.to_dict(), ensure_ascii=False)

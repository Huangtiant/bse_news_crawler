"""
PDF 提取 pipeline。

职责：给定一条 bseinfo 公告 record，完成：
  1. 下载 PDF 到 data/raw/
  2. 提取文本
  3. 清洗文本
  4. 保存 txt 到 data/text/
  5. 通过 RecordUpdater 写回 record

对外入口：
    process_record(record)           处理单条
    process_records(records)         批量处理，跳过已完成的
"""
from __future__ import annotations

import requests
from pathlib import Path

from models.record import ContentStatus, Record
from storage import save_text
from storage.path_builder import PathBuilder
from updaters.record_updater import RecordUpdater, UpdatePayload
from extractors.pdf_extractor import extract_text, PdfExtractionError
from cleaner import normalize_text, remove_noise_lines


_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}


def process_record(
    record: Record,
    path_builder: PathBuilder | None = None,
    updater: RecordUpdater | None = None,
    timeout: int = 30,
) -> str:
    """
    对单条 bseinfo 公告 record 执行完整的 PDF 提取流程。

    返回最终的 content_status（"cleaned" 或 "failed"）。
    """
    pb  = path_builder or PathBuilder()
    upd = updater      or RecordUpdater()

    if not record.pdf_url:
        print(f"[pdf_pipeline] 跳过（无 pdf_url）: {record.id}")
        return record.content_status

    if record.content_status in {ContentStatus.CLEANED, ContentStatus.SUMMARIZED}:
        print(f"[pdf_pipeline] 跳过（已完成）: {record.id}")
        return record.content_status

    pdf_path = pb.raw_file("bseinfo", record.publish_date, record.id, ".pdf")
    txt_rel  = pb.text_file_rel("bseinfo", record.publish_date, record.id)
    txt_path = pb.to_abs(txt_rel)

    # ── 步骤 1：下载 PDF ──────────────────────────────────────────────
    if not pdf_path.exists():
        try:
            resp = requests.get(record.pdf_url, headers=_HEADERS, timeout=timeout)
            resp.raise_for_status()
            pdf_path.parent.mkdir(parents=True, exist_ok=True)
            pdf_path.write_bytes(resp.content)
            upd.update_by_id(
                record.id, record.publish_date,
                UpdatePayload(
                    content_status=ContentStatus.DOWNLOADED,
                    raw_file_path=pb.raw_file_rel("bseinfo", record.publish_date, record.id, ".pdf"),
                ),
            )
            print(f"[pdf_pipeline] 下载完成: {pdf_path.name}")
        except Exception as e:
            print(f"[pdf_pipeline] 下载失败 {record.id}: {e}")
            upd.update_by_id(
                record.id, record.publish_date,
                UpdatePayload(
                    content_status=ContentStatus.FAILED,
                ),
            )
            return ContentStatus.FAILED

    # ── 步骤 2：提取文本 ──────────────────────────────────────────────
    try:
        raw_text = extract_text(pdf_path)
    except PdfExtractionError as e:
        print(f"[pdf_pipeline] 提取失败 {record.id}: {e}")
        upd.update_by_id(
            record.id, record.publish_date,
            UpdatePayload(
                content_status=ContentStatus.FAILED,
            ),
        )
        return ContentStatus.FAILED

    # ── 步骤 3：清洗 ──────────────────────────────────────────────────
    clean_text = normalize_text(remove_noise_lines(raw_text))

    # ── 步骤 4：保存 txt ──────────────────────────────────────────────
    save_text(txt_path, clean_text)

    # ── 步骤 5：写回 record ───────────────────────────────────────────
    upd.update_content(
        record_id=record.id,
        publish_date=record.publish_date,
        content_text=clean_text,
        text_file_path=txt_rel,
    )
    print(f"[pdf_pipeline] 完成: {record.id} ({len(clean_text)} 字)")
    return ContentStatus.CLEANED


def process_records(
    records: list[Record],
    path_builder: PathBuilder | None = None,
    updater: RecordUpdater | None = None,
) -> dict[str, int]:
    """
    批量处理，跳过已完成的记录。
    返回 {"cleaned": N, "failed": M, "skipped": K}。
    """
    pb  = path_builder or PathBuilder()
    upd = updater      or RecordUpdater()

    counts = {"cleaned": 0, "failed": 0, "skipped": 0}
    for r in records:
        status = process_record(r, pb, upd)
        if status == ContentStatus.CLEANED:
            counts["cleaned"] += 1
        elif status == ContentStatus.FAILED:
            counts["failed"] += 1
        else:
            counts["skipped"] += 1

    return counts

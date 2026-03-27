"""
阶段 7 验证脚本：测试幂等写入和去重。
运行方式：python src/storage/demo_upsert.py
"""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.record import Record, ContentStatus
from storage.path_builder import PathBuilder
from storage.jsonl_store import JsonlStore


def _base_record(pdf_url: str, company: str, date: str = "2026-03-27") -> Record:
    return Record(
        id=Record.make_id("bseinfo", pdf_url),
        source="bseinfo", source_type="official", record_type="announcement",
        company_code="920445", company_name=company,
        title=f"[年度报告]{company}:2025年报",
        category="年度报告", publish_date=date,
        fetch_time=Record.now_iso(),
        pdf_url=pdf_url,
        content_status=ContentStatus.EMPTY,
    )


with tempfile.TemporaryDirectory() as tmp:
    pb    = PathBuilder(data_root=Path(tmp))
    store = JsonlStore(pb)

    r1 = _base_record("https://example.com/a.pdf", "柔灵科技")
    r2 = _base_record("https://example.com/b.pdf", "东方时尚")

    # ── 测试 1：首次写入，两条都是 insert ──────────────────────────
    stats = store.upsert_records([r1, r2])
    print(f"首次写入: {stats}")
    assert stats == {"inserted": 2, "updated": 0}

    records = store.read_by_date("2026-03-27")
    assert len(records) == 2, f"expected 2, got {len(records)}"
    print(f"文件中记录数: {len(records)} (应为 2)")

    # ── 测试 2：重复写入相同记录，不应产生重复行 ───────────────────
    stats2 = store.upsert_records([r1, r2])
    print(f"重复写入: {stats2}")
    assert stats2 == {"inserted": 0, "updated": 2}

    records2 = store.read_by_date("2026-03-27")
    assert len(records2) == 2, f"expected 2, got {len(records2)}"
    print(f"重复写入后记录数: {len(records2)} (应仍为 2，无重复)")

    # ── 测试 3：upsert 时更新可更新字段 ────────────────────────────
    r1_updated = _base_record("https://example.com/a.pdf", "柔灵科技")
    r1_updated.content_text   = "这是提取后的正文文本"
    r1_updated.content_status = ContentStatus.CLEANED
    r1_updated.text_file_path = "text/bseinfo/2026-03-27/abc123.txt"
    r1_updated.summary        = "年度业绩稳健增长"

    result = store.upsert_record(r1_updated)
    assert result is False, "应返回 False 表示更新"

    records3 = store.read_by_date("2026-03-27")
    assert len(records3) == 2, f"更新后记录数应仍为 2，got {len(records3)}"

    updated = next(r for r in records3 if r.id == r1.id)
    assert updated.content_status == ContentStatus.CLEANED
    assert updated.content_text   == "这是提取后的正文文本"
    assert updated.summary        == "年度业绩稳健增长"
    # 不可更新字段保持原值
    assert updated.company_name   == "柔灵科技"
    assert updated.title          == "[年度报告]柔灵科技:2025年报"
    print(f"更新后 content_status: {updated.content_status} (应为 cleaned)")
    print(f"更新后 summary: {updated.summary}")

    # ── 测试 4：新增第三条，文件应有 3 条 ──────────────────────────
    r3 = _base_record("https://example.com/c.pdf", "华图山鼎")
    result3 = store.upsert_record(r3)
    assert result3 is True, "应返回 True 表示新增"

    records4 = store.read_by_date("2026-03-27")
    assert len(records4) == 3, f"expected 3, got {len(records4)}"
    print(f"新增第三条后记录数: {len(records4)} (应为 3)")

    print("\nAll assertions passed OK")

"""
阶段 8 验证脚本：演示 PDF 文本回填和摘要回填流程。
运行方式：python src/updaters/demo_updater.py
"""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.record import Record, ContentStatus
from storage.path_builder import PathBuilder
from storage.jsonl_store import JsonlStore
from updaters.record_updater import RecordUpdater, UpdatePayload


def _make_pdf_record(pdf_url: str, company: str) -> Record:
    return Record(
        id=Record.make_id("bseinfo", pdf_url),
        source="bseinfo", source_type="official", record_type="announcement",
        company_code="920445", company_name=company,
        title=f"[年度报告]{company}:2025年报",
        category="年度报告", publish_date="2026-03-27",
        fetch_time=Record.now_iso(),
        pdf_url=pdf_url,
        content_status=ContentStatus.EMPTY,
    )


with tempfile.TemporaryDirectory() as tmp:
    pb      = PathBuilder(data_root=Path(tmp))
    store   = JsonlStore(pb)
    updater = RecordUpdater(store)

    # ── 准备：写入两条初始记录（content 为空）──────────────────────
    r1 = _make_pdf_record("https://example.com/a.pdf", "柔灵科技")
    r2 = _make_pdf_record("https://example.com/b.pdf", "东方时尚")
    store.upsert_records([r1, r2])

    records = store.read_by_date("2026-03-27")
    assert all(r.content_status == ContentStatus.EMPTY for r in records)
    print(f"初始写入: {len(records)} 条，content_status 均为 empty")

    # ── 场景一：PDF 文本提取完成，回填 content_text ────────────────
    extracted_text = "柔灵科技2025年实现营业收入3.2亿元，同比增长18%。\n净利润4200万元，同比增长25%。"
    txt_rel = pb.text_file_rel("bseinfo", "2026-03-27", r1.id)

    ok = updater.update_content(
        record_id=r1.id,
        publish_date="2026-03-27",
        content_text=extracted_text,
        text_file_path=txt_rel,
    )
    assert ok is True, "应返回 True 表示找到并更新"

    records2 = store.read_by_date("2026-03-27")
    updated_r1 = next(r for r in records2 if r.id == r1.id)

    assert updated_r1.content_text   == extracted_text
    assert updated_r1.content_status == ContentStatus.CLEANED
    assert updated_r1.text_file_path == txt_rel
    assert len(updated_r1.content_hash) == 16       # sha256[:16]
    assert len(records2) == 2                        # 总数不变
    print(f"文本回填后 content_status: {updated_r1.content_status}")
    print(f"content_hash: {updated_r1.content_hash}")

    # r2 不受影响
    unchanged_r2 = next(r for r in records2 if r.id == r2.id)
    assert unchanged_r2.content_status == ContentStatus.EMPTY
    print(f"r2 未受影响，仍为: {unchanged_r2.content_status}")

    # ── 场景二：AI 摘要生成，回填 summary 和 tags ──────────────────
    ok2 = updater.update_summary(
        record_id=r1.id,
        publish_date="2026-03-27",
        summary="柔灵科技2025年营收增长18%，净利润增长25%，业绩稳健。",
        tags=["年度报告", "营收增长", "北交所"],
    )
    assert ok2 is True

    records3 = store.read_by_date("2026-03-27")
    final_r1 = next(r for r in records3 if r.id == r1.id)

    assert final_r1.summary == "柔灵科技2025年营收增长18%，净利润增长25%，业绩稳健。"
    assert final_r1.tags    == ["年度报告", "营收增长", "北交所"]
    assert final_r1.content_status == ContentStatus.SUMMARIZED
    assert final_r1.content_text == extracted_text  # 正文不受影响
    print(f"摘要回填后 content_status: {final_r1.content_status}")
    print(f"tags: {final_r1.tags}")

    # ── 场景三：UpdatePayload 精确控制（只更新 raw_file_path）──────
    ok3 = updater.update_by_id(
        record_id=r2.id,
        publish_date="2026-03-27",
        payload=UpdatePayload(raw_file_path="raw/bseinfo/2026-03-27/xyz.pdf"),
    )
    assert ok3 is True
    records4 = store.read_by_date("2026-03-27")
    final_r2 = next(r for r in records4 if r.id == r2.id)
    assert final_r2.raw_file_path  == "raw/bseinfo/2026-03-27/xyz.pdf"
    assert final_r2.content_status == ContentStatus.EMPTY   # 其他字段不动
    print(f"r2 raw_file_path 已更新: {final_r2.raw_file_path}")

    # ── 场景四：id 不存在时返回 False ──────────────────────────────
    ok4 = updater.update_content(
        record_id="nonexistent_id",
        publish_date="2026-03-27",
        content_text="some text",
    )
    assert ok4 is False
    print("不存在的 id 返回 False: OK")

    print("\nAll assertions passed OK")

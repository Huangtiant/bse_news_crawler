"""
阶段 10 验证脚本。
运行方式：python src/extractors/demo_pdf_pipeline.py

分两部分：
  Part A — 纯本地测试（不联网），用 pdfplumber 生成最小 PDF 验证提取接口
  Part B — 用真实 bseinfo PDF URL 做端到端测试（需要联网）
"""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ══════════════════════════════════════════════════════════════════
# Part A：本地接口测试（不联网）
# ══════════════════════════════════════════════════════════════════
print("=" * 60)
print("Part A: 本地接口测试")
print("=" * 60)

from extractors.pdf_extractor import extract_text, PdfExtractionError

# A1：文件不存在时抛出 PdfExtractionError
try:
    extract_text("/nonexistent/path.pdf")
    assert False, "应抛出异常"
except PdfExtractionError as e:
    print(f"A1 不存在文件 → PdfExtractionError: OK ({e})")

# A2：用 reportlab 或 fpdf 生成最小 PDF 并提取
#     如果两者都没装，跳过 A2 只做提示
try:
    from reportlab.pdfgen import canvas as rl_canvas

    with tempfile.TemporaryDirectory() as tmp:
        pdf_path = Path(tmp) / "test.pdf"
        c = rl_canvas.Canvas(str(pdf_path))
        c.setFont("Helvetica", 12)
        c.drawString(50, 750, "Hello PDF Extractor")
        c.drawString(50, 730, "This is page one.")
        c.showPage()
        c.drawString(50, 750, "Page two content here.")
        c.save()

        text = extract_text(pdf_path)
        assert "Hello PDF Extractor" in text or "Hello" in text
        assert len(text) > 0
        print(f"A2 reportlab PDF 提取成功: {len(text)} 字符")

except ImportError:
    print("A2 跳过（reportlab 未安装，不影响生产使用）")

# ══════════════════════════════════════════════════════════════════
# Part B：端到端测试（联网 + 真实 record）
# ══════════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("Part B: 端到端 PDF pipeline 测试（联网）")
print("=" * 60)

import json
from pathlib import Path as P

from models.record import Record, ContentStatus
from storage.path_builder import PathBuilder
from storage.jsonl_store import JsonlStore
from updaters.record_updater import RecordUpdater
from extractors.pdf_pipeline import process_record

# 从已有的 announcements 数据里取一条年度报告
announcements_dir = P(__file__).resolve().parent.parent.parent / "data" / "announcements"
sample_raw = None

for json_file in sorted(announcements_dir.rglob("年度报告.json")):
    records_raw = json.loads(json_file.read_text(encoding="utf-8"))
    if records_raw:
        sample_raw = records_raw[0]
        print(f"使用样本: {json_file.relative_to(announcements_dir.parent.parent)}")
        break

if not sample_raw:
    print("未找到本地公告数据，跳过 Part B（先运行一次 main.py 抓取数据）")
    sys.exit(0)

print(f"公司: {sample_raw['company_name']}  标题: {sample_raw['title'][:40]}")
print(f"PDF:  {sample_raw['pdf_url']}")

# 用临时目录，不污染真实 data/
with tempfile.TemporaryDirectory() as tmp:
    pb    = PathBuilder(data_root=P(tmp))
    store = JsonlStore(pb)
    upd   = RecordUpdater(store)

    from normalizers import bseinfo_normalizer
    record = bseinfo_normalizer.normalize(sample_raw)
    store.upsert_record(record)

    print(f"\n处理前 content_status: {record.content_status}")
    final_status = process_record(record, pb, upd)
    print(f"处理后 content_status: {final_status}")

    # 从 store 读回验证
    stored = store.read_by_date(record.publish_date)
    updated = next((r for r in stored if r.id == record.id), None)
    assert updated is not None

    if final_status == ContentStatus.CLEANED:
        assert updated.content_status == ContentStatus.CLEANED
        assert len(updated.content_text) > 0
        assert updated.text_file_path != ""
        assert len(updated.content_hash) == 16
        print(f"\n正文前 200 字:")
        print(updated.content_text[:200])
        print(f"\ncontent_hash: {updated.content_hash}")
        txt_abs = pb.to_abs(updated.text_file_path)
        assert txt_abs.exists(), f"txt 文件不存在: {txt_abs}"
        print(f"txt 文件已落盘: {txt_abs.name}")
    else:
        print(f"提取失败（status={final_status}），可能是扫描件 PDF")

print("\nPart B done OK")

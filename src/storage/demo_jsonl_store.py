"""
阶段 3 验证脚本：写入两条记录，按日期读出并验证。
运行方式：python src/storage/demo_jsonl_store.py
"""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.record import Record, ContentStatus
from storage.path_builder import PathBuilder
from storage.jsonl_store import JsonlStore

# 用临时目录，不污染真实 data/
with tempfile.TemporaryDirectory() as tmp:
    pb    = PathBuilder(data_root=Path(tmp))
    store = JsonlStore(pb)

    # ── 构造两条测试记录 ──
    r1 = Record(
        id=Record.make_id("bseinfo", "https://example.com/a.pdf"),
        source="bseinfo",
        source_type="official",
        record_type="announcement",
        company_code="920445",
        company_name="柔灵科技",
        title="[年度报告摘要]柔灵科技:2025年年度报告摘要",
        category="年度报告",
        publish_date="2026-03-27",
        fetch_time=Record.now_iso(),
        pdf_url="https://example.com/a.pdf",
        content_status=ContentStatus.EMPTY,
    )

    r2 = Record(
        id=Record.make_id("eastmoney", "AP202603271820801732"),
        source="eastmoney",
        source_type="media",
        record_type="article",
        title="某品牌ODM纺织企业2025年营收增21%",
        category="研报",
        publish_date="2026-03-27",
        fetch_time=Record.now_iso(),
        url="http://data.eastmoney.com/report/zw_stock.jshtml?infocode=AP202603271820801732",
        content_status=ContentStatus.EMPTY,
        extra={"infocode": "AP202603271820801732"},
    )

    r3 = Record(
        id=Record.make_id("bseinfo", "https://example.com/b.pdf"),
        source="bseinfo",
        source_type="official",
        record_type="announcement",
        company_code="833819",
        company_name="东方时尚",
        title="[半年度报告]东方时尚:2025年半年度报告",
        category="半年度报告",
        publish_date="2026-03-28",       # 不同日期
        fetch_time=Record.now_iso(),
        pdf_url="https://example.com/b.pdf",
        content_status=ContentStatus.EMPTY,
    )

    # ── 写入 ──
    store.append_record(r1)
    store.append_records([r2, r3])   # r2 → 03-27，r3 → 03-28

    # ── 验证生成的文件 ──
    files = sorted(Path(tmp).rglob("*.jsonl"))
    print(f"生成 {len(files)} 个 JSONL 文件：")
    for f in files:
        print(f"  {f.relative_to(tmp)}")

    # ── 按日期读取 ──
    day27 = store.read_by_date("2026-03-27")
    day28 = store.read_by_date("2026-03-28")
    day99 = store.read_by_date("2099-01-01")   # 不存在的日期

    print(f"\nread_by_date('2026-03-27') → {len(day27)} 条")
    for r in day27:
        print(f"  [{r.source}] {r.title[:30]}")

    print(f"\nread_by_date('2026-03-28') → {len(day28)} 条")
    for r in day28:
        print(f"  [{r.source}] {r.title[:30]}")

    print(f"\nread_by_date('2099-01-01') → {len(day99)} 条（应为 0）")

    # ── 按日期范围读取 ──
    ranged = store.read_by_date_range("2026-03-27", "2026-03-28")
    print(f"\nread_by_date_range('2026-03-27', '2026-03-28') → {len(ranged)} 条（应为 3）")

    # ── 断言 ──
    assert len(day27) == 2,  f"expected 2, got {len(day27)}"
    assert len(day28) == 1,  f"expected 1, got {len(day28)}"
    assert len(day99) == 0,  f"expected 0, got {len(day99)}"
    assert len(ranged) == 3, f"expected 3, got {len(ranged)}"
    assert day27[0].id == r1.id
    assert day27[1].id == r2.id
    assert day28[0].id == r3.id

    # 验证 from_dict 还原字段完整
    assert day27[0].company_code == "920445"
    assert day27[1].extra["infocode"] == "AP202603271820801732"

    print("\nAll assertions passed OK")

"""
阶段 6 验证脚本：演示 RecordReader 的各种查询方式。
运行方式：python src/readers/demo_reader.py

如果 data/records/ 还没有真实数据，脚本会先写入若干测试记录再演示。
"""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.record import Record, ContentStatus
from storage.path_builder import PathBuilder
from storage.jsonl_store import JsonlStore
from readers.record_reader import RecordReader


# ── 准备测试数据 ────────────────────────────────────────────────────

def _make_records() -> list[Record]:
    return [
        Record(
            id=Record.make_id("bseinfo", "https://example.com/a.pdf"),
            source="bseinfo", source_type="official", record_type="announcement",
            company_code="920445", company_name="柔灵科技",
            title="[年度报告摘要]柔灵科技:2025年报",
            category="年度报告", publish_date="2026-03-27",
            fetch_time=Record.now_iso(),
            pdf_url="https://example.com/a.pdf",
            content_status=ContentStatus.EMPTY,
        ),
        Record(
            id=Record.make_id("bseinfo", "https://example.com/b.pdf"),
            source="bseinfo", source_type="official", record_type="announcement",
            company_code="833819", company_name="东方时尚",
            title="[半年报]东方时尚:2025年中",
            category="半年度报告", publish_date="2026-03-27",
            fetch_time=Record.now_iso(),
            pdf_url="https://example.com/b.pdf",
            content_status=ContentStatus.EMPTY,
        ),
        Record(
            id=Record.make_id("bseinfo", "https://example.com/c.pdf"),
            source="bseinfo", source_type="official", record_type="announcement",
            company_code="920445", company_name="柔灵科技",
            title="[业绩快报]柔灵科技:2025年业绩快报",
            category="业绩预告、业绩快报类", publish_date="2026-03-27",
            fetch_time=Record.now_iso(),
            pdf_url="https://example.com/c.pdf",
            content_status=ContentStatus.DOWNLOADED,
        ),
        Record(
            id=Record.make_id("eastmoney", "AP202603271820801732"),
            source="eastmoney", source_type="media", record_type="article",
            title="北交所最新研报：某品牌ODM纺织企业2025年营收增21%",
            category="研报", publish_date="2026-03-27",
            fetch_time=Record.now_iso(),
            url="http://data.eastmoney.com/report/zw_stock.jshtml?infocode=AP202603271820801732",
            content_status=ContentStatus.CLEANED,
            extra={"infocode": "AP202603271820801732"},
        ),
        Record(
            id=Record.make_id("bseinfo", "https://example.com/d.pdf"),
            source="bseinfo", source_type="official", record_type="announcement",
            company_code="833819", company_name="东方时尚",
            title="[年度报告]东方时尚:2025年报",
            category="年度报告", publish_date="2026-03-28",   # 不同日期
            fetch_time=Record.now_iso(),
            pdf_url="https://example.com/d.pdf",
            content_status=ContentStatus.EMPTY,
        ),
    ]


with tempfile.TemporaryDirectory() as tmp:
    pb    = PathBuilder(data_root=Path(tmp))
    store = JsonlStore(pb)
    store.append_records(_make_records())

    reader = RecordReader(store)

    # ── 演示 1：读取某一天全部记录 ──────────────────────────────────
    all_27 = reader.by_date("2026-03-27").fetch()
    print(f"by_date('2026-03-27') → {len(all_27)} 条（应为 4）")
    for r in all_27:
        print(f"  [{r.source}] [{r.category}] {r.title[:35]}")

    # ── 演示 2：按来源过滤 ──────────────────────────────────────────
    bseinfo_only = reader.by_date("2026-03-27").source("bseinfo").fetch()
    print(f"\n.source('bseinfo') → {len(bseinfo_only)} 条（应为 3）")

    eastmoney_only = reader.by_date("2026-03-27").source("eastmoney").fetch()
    print(f".source('eastmoney') → {len(eastmoney_only)} 条（应为 1）")

    # ── 演示 3：按公司代码过滤 ──────────────────────────────────────
    lingling = reader.by_date("2026-03-27").company_code("920445").fetch()
    print(f"\n.company_code('920445') → {len(lingling)} 条（应为 2）")
    for r in lingling:
        print(f"  {r.company_name} / {r.category}")

    # ── 演示 4：按公司名称模糊匹配 ──────────────────────────────────
    dongfang = reader.by_date("2026-03-27").company_name("东方").fetch()
    print(f"\n.company_name('东方') → {len(dongfang)} 条（应为 1）")

    # ── 演示 5：按分类过滤 ──────────────────────────────────────────
    annual = reader.by_date_range("2026-03-27", "2026-03-28").category("年度报告").fetch()
    print(f"\ndate_range + .category('年度报告') → {len(annual)} 条（应为 2，跨两天）")
    for r in annual:
        print(f"  {r.publish_date} / {r.company_name}")

    # ── 演示 6：多条件组合 ──────────────────────────────────────────
    combo = (
        reader.by_date("2026-03-27")
        .source("bseinfo")
        .content_status(ContentStatus.DOWNLOADED)
        .fetch()
    )
    print(f"\nsource=bseinfo + content_status=downloaded → {len(combo)} 条（应为 1）")
    if combo:
        print(f"  {combo[0].title}")

    # ── 断言 ────────────────────────────────────────────────────────
    assert len(all_27)        == 4
    assert len(bseinfo_only)  == 3
    assert len(eastmoney_only) == 1
    assert len(lingling)      == 2
    assert len(dongfang)      == 1
    assert len(annual)        == 2
    assert len(combo)         == 1

    print("\nAll assertions passed OK")

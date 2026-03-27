"""
阶段 9 验证脚本：演示公告过滤。
运行方式：python src/filters/demo_filter.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.record import Record, ContentStatus
from filters.announcement_filter import filter_announcements, summarize


def _rec(category: str, title: str, source: str = "bseinfo") -> Record:
    return Record(
        id=Record.make_id(source, title),
        source=source,
        source_type="official" if source == "bseinfo" else "media",
        record_type="announcement",
        company_code="920445",
        company_name="测试公司",
        title=title,
        category=category,
        publish_date="2026-03-27",
        fetch_time=Record.now_iso(),
        content_status=ContentStatus.EMPTY,
    )


all_records = [
    # 白名单分类 → 全部放行
    _rec("年度报告",           "[年度报告]甲公司:2025年报"),
    _rec("半年度报告",         "[半年报]乙公司:2025年中"),
    _rec("一季度报告",         "[一季报]丙公司:2025Q1"),
    _rec("三季度报告",         "[三季报]丁公司:2025Q3"),
    _rec("业绩预告、业绩快报类", "[业绩快报]戊公司:2025年快报"),

    # 公开发行类 + 标题命中关键词 → 放行
    _rec("公开发行类", "己公司：关于定向增发预案的公告"),
    _rec("公开发行类", "庚公司：2025年股权激励计划草案"),

    # 公开发行类 + 标题无关键词 → 排除
    _rec("公开发行类", "辛公司：关于独立董事换届选举的公告"),
    _rec("公开发行类", "壬公司：2025年度监事会工作报告"),

    # 非 bseinfo 来源 → 排除
    _rec("年度报告", "东方财富研报：某公司年报解读", source="eastmoney"),

    # 未知分类 → 排除
    _rec("临时公告", "癸公司：关于签署战略合作协议的公告"),
]

print(f"输入记录总数: {len(all_records)}")
print()

candidates = filter_announcements(all_records)

print(f"过滤后候选记录: {len(candidates)} 条（应为 7）")
print()
for r in candidates:
    print(f"  [{r.category}] {r.title}")

print()
print("── 分类统计 ──")
for cat, cnt in summarize(candidates).items():
    print(f"  {cat}: {cnt} 条")

print()
# 被排除的
excluded = [r for r in all_records if r not in candidates]
print(f"被排除: {len(excluded)} 条（应为 4）")
for r in excluded:
    print(f"  [{r.source}][{r.category}] {r.title}")

# ── 断言 ──────────────────────────────────────────────────────────
assert len(candidates) == 7, f"expected 7, got {len(candidates)}"
assert len(excluded)   == 4, f"expected 4, got {len(excluded)}"

# 白名单分类全部通过
whitelist_passed = [r for r in candidates if r.category in {
    "年度报告", "半年度报告", "一季度报告", "三季度报告", "业绩预告、业绩快报类"
}]
assert len(whitelist_passed) == 5

# 关键词匹配通过 2 条
keyword_passed = [r for r in candidates if r.category == "公开发行类"]
assert len(keyword_passed) == 2

# eastmoney 来源被排除
assert all(r.source == "bseinfo" for r in candidates)

print("\nAll assertions passed OK")

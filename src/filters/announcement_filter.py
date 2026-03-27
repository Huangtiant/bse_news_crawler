"""
北交所公告过滤层。

职责：从全量公告记录中筛选出有分析价值的候选记录。
输入：Record 列表（来自 RecordReader 或 JsonlStore）
输出：过滤后的 Record 列表（内存，不落盘）

过滤策略：
  1. 白名单分类：直接放行的公告类型
  2. 关键词分类：标题含特定关键词才放行（用于"公开发行类"等宽泛分类）
  3. 来源过滤：只处理 bseinfo 来源的公告
"""
from __future__ import annotations

from models.record import Record


# ── 白名单：整个分类直接放行 ────────────────────────────────────────
WHITELIST_CATEGORIES = {
    "年度报告",
    "半年度报告",
    "一季度报告",
    "三季度报告",
    "业绩预告、业绩快报类",
}

# ── 关键词白名单：标题包含任一关键词才放行 ──────────────────────────
# 适用于"公开发行类"等包含大量无关公告的宽泛分类
KEYWORD_CATEGORIES: dict[str, list[str]] = {
    "公开发行类": [
        "定向增发",
        "公开发行",
        "股权激励",
        "可转换债券",
        "优先股",
    ],
}


def filter_announcements(records: list[Record]) -> list[Record]:
    """
    主过滤函数。对输入列表逐条判断，返回通过过滤的记录。

    过滤规则优先级：
      1. 非 bseinfo 来源 → 直接排除
      2. category 在白名单 → 放行
      3. category 在关键词分类且标题命中关键词 → 放行
      4. 其余 → 排除
    """
    result = []
    for r in records:
        if r.source != "bseinfo":
            continue
        if _passes(r):
            result.append(r)
    return result


def _passes(r: Record) -> bool:
    if r.category in WHITELIST_CATEGORIES:
        return True

    keywords = KEYWORD_CATEGORIES.get(r.category)
    if keywords and any(kw in r.title for kw in keywords):
        return True

    return False


# ── 统计工具 ────────────────────────────────────────────────────────

def summarize(records: list[Record]) -> dict[str, int]:
    """返回按 category 分组的计数，便于查看过滤结果分布。"""
    counts: dict[str, int] = {}
    for r in records:
        counts[r.category] = counts.get(r.category, 0) + 1
    return dict(sorted(counts.items()))

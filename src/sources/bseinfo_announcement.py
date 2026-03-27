"""
抓取北京证券交易所「上市公司公告」列表页，提取指定类型公告的 PDF 直链。

页面结构（实测）：
  - 公告类型筛选：span[title="年度报告"][code="distype"]，选中后获得 span_choose class
  - 列表：#table tbody tr，每行 5 列：代码 | 简称 | 标题+PDF链接 | 图标 | 日期
  - 分页：.mw-paging a.next（最后一页时该元素消失，变为 span）

使用方式：
  fetch_all_announcements()              抓取所有预设类型，返回 {类型: [记录]} 字典
  fetch_announcements_by_type(page, t)   在已有 page 上抓取单一类型
"""
from typing import List, Dict
from playwright.sync_api import sync_playwright, Page

BASE_URL = "https://www.bseinfo.net"
ANNOUNCEMENT_URL = f"{BASE_URL}/disclosure/announcement.html"

# 页面上 span[code="distype"] 的 title 属性值，需与实际 DOM 完全一致
ANNOUNCEMENT_TYPES = [
    "年度报告",
    "半年度报告",
    "一季度报告",
    "三季度报告",
    "业绩预告、业绩快报类",
    "公开发行类",
]


def _select_announcement_type(page: Page, title: str) -> None:
    """
    选择公告分类筛选项（如「年度报告」）。
    等待 span 获得 span_choose class 确认 JS 已注册选中状态。
    """
    page.wait_for_selector(f'span[code="distype"][title="{title}"]', timeout=10000)
    page.evaluate(
        """(title) => {
            document.querySelector(
                `span[code="distype"][title="${title}"]`
            ).click();
        }""",
        title,
    )
    page.wait_for_selector(
        f'span[code="distype"][title="{title}"].span_choose',
        timeout=5000,
    )


def _submit_query(page: Page) -> None:
    """点击查询按钮并等待列表刷新。"""
    page.click("button.cx")
    page.wait_for_load_state("networkidle", timeout=20000)
    try:
        page.wait_for_selector("#table tbody tr", timeout=10000)
    except Exception:
        pass


def _extract_rows_from_page(page: Page) -> List[Dict]:
    """从当前页面的表格提取所有公告行。"""
    return page.eval_on_selector_all(
        "#table tbody tr",
        """
        rows => rows.map(tr => {
            const cells = tr.querySelectorAll('td');
            if (cells.length < 5) return null;
            const linkEl = cells[2].querySelector('a');
            if (!linkEl) return null;
            return {
                company_code: (cells[0].innerText || '').trim(),
                company_name: (cells[1].innerText || '').trim(),
                title:        (linkEl.innerText   || '').trim(),
                pdf_url:      linkEl.href || '',
                date:         (cells[4].innerText || '').trim(),
            };
        }).filter(Boolean)
        """,
    )


def _has_next_page(page: Page) -> bool:
    """判断是否存在下一页（最后一页时 a.next 消失，变为 span）。"""
    return page.query_selector(".mw-paging a.next") is not None


def _go_next_page(page: Page) -> None:
    """点击下一页并等待列表真正刷新（内容变化后再返回）。"""
    # 记录翻页前第一行的文字，用于检测内容是否已更新
    first_row_before = page.text_content("#table tbody tr:first-child") or ""

    page.click(".mw-paging a.next")

    # 等待内容实际变化，而不是依赖 networkidle（networkidle 可能在 DOM 更新前触发）
    page.wait_for_function(
        """(prev) => {
            const tr = document.querySelector('#table tbody tr:first-child');
            if (!tr) return false;
            const text = tr.innerText.trim();
            if (!text || text === prev) return false;
            if (text.includes('加载中') || text.includes('请稍后')) return false;
            // 确保是真实数据行：表格有多个 td
            const tds = tr.querySelectorAll('td');
            return tds.length >= 4;
        }""",
        arg=first_row_before,
        timeout=20000,
    )


def fetch_announcements_by_type(page: Page, announcement_type: str) -> List[Dict]:
    """
    在已有 page 上抓取单一类型的所有公告，复用浏览器实例避免重复启动。
    每条记录附带 report_type 字段。

    返回列表，每项包含：
        company_code  股票代码
        company_name  公司简称
        title         公告标题
        pdf_url       PDF 完整 URL
        date          发布日期（YYYY-MM-DD）
        report_type   公告类型（与传入参数相同）
    """
    results: List[Dict] = []

    # 每次重新导航，避免上一次筛选状态残留
    page.goto(ANNOUNCEMENT_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_selector("#table tbody tr", timeout=20000)

    _select_announcement_type(page, announcement_type)
    _submit_query(page)

    total_tip = (page.text_content(".paging-tips-right") or "").strip()
    print(f"[bseinfo] [{announcement_type}] 筛选结果: {total_tip}")

    page_num = 1
    while True:
        rows = _extract_rows_from_page(page)
        for row in rows:
            row["report_type"] = announcement_type
        results.extend(rows)
        print(f"[bseinfo] [{announcement_type}] 第 {page_num} 页，本页 {len(rows)} 条，累计 {len(results)} 条")

        if not _has_next_page(page):
            break

        _go_next_page(page)
        page_num += 1

    print(f"[bseinfo] [{announcement_type}] 抓取完成，共 {len(results)} 条")
    return results


def fetch_all_announcements(types: List[str] = None) -> Dict[str, List[Dict]]:
    """
    依次抓取多种公告类型，复用同一浏览器实例。

    参数：
        types  要抓取的类型列表，默认使用 ANNOUNCEMENT_TYPES 全部类型

    返回：
        {announcement_type: [records]} 字典
    """
    if types is None:
        types = ANNOUNCEMENT_TYPES

    all_results: Dict[str, List[Dict]] = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 900})

        for t in types:
            try:
                all_results[t] = fetch_announcements_by_type(page, t)
            except Exception as e:
                print(f"[bseinfo] [{t}] 抓取失败: {e}")
                all_results[t] = []

        browser.close()

    return all_results


if __name__ == "__main__":
    results = fetch_all_announcements()
    for t, rows in results.items():
        print(f"[{t}] {len(rows)} 条")
        for r in rows[:2]:
            print(" ", r)

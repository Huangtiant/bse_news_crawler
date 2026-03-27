"""
抓取北京证券交易所「上市公司公告」列表页，提取所有年度报告的 PDF 直链。

页面结构（实测）：
  - 公告类型筛选：span[title="年度报告"][code="distype"]，选中后获得 span_choose class
  - 列表：#table tbody tr，每行 5 列：代码 | 简称 | 标题+PDF链接 | 图标 | 日期
  - 分页：.mw-paging a.next（最后一页时该元素消失，变为 span）

使用方式：
  直接调用 fetch_annual_report_pdfs()，返回所有条目（含发布日期）。
  由调用方按 date 字段落盘，存储时去重。
"""
from typing import List, Dict
from playwright.sync_api import sync_playwright, Page

BASE_URL = "https://www.bseinfo.net"
ANNOUNCEMENT_URL = f"{BASE_URL}/disclosure/announcement.html"


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
    """点击下一页并等待列表刷新。"""
    page.click(".mw-paging a.next")
    page.wait_for_load_state("networkidle", timeout=20000)
    page.wait_for_selector("#table tbody tr", timeout=15000)


def fetch_annual_report_pdfs() -> List[Dict]:
    """
    抓取所有「年度报告」公告的 PDF 直链。
    默认展示最近一个月，翻页获取全部条目。

    返回列表，每项包含：
        company_code  股票代码
        company_name  公司简称
        title         公告标题
        pdf_url       PDF 完整 URL
        date          发布日期（YYYY-MM-DD）
    """
    results: List[Dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 900})

        page.goto(ANNOUNCEMENT_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_selector("#table tbody tr", timeout=20000)

        _select_announcement_type(page, "年度报告")
        _submit_query(page)

        total_tip = (page.text_content(".paging-tips-right") or "").strip()
        print(f"[bseinfo] 筛选结果: {total_tip}")

        page_num = 1
        while True:
            rows = _extract_rows_from_page(page)
            results.extend(rows)
            print(f"[bseinfo] 第 {page_num} 页，本页 {len(rows)} 条，累计 {len(results)} 条")

            if not _has_next_page(page):
                break

            _go_next_page(page)
            page_num += 1

        browser.close()

    print(f"[bseinfo] 抓取完成，共 {len(results)} 条")
    return results

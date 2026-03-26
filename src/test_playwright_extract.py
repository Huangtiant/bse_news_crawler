from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright


def main():
    keyword = "北交所"
    url = f"https://so.eastmoney.com/yanbao/s?keyword={keyword}"

    project_root = Path(__file__).resolve().parent.parent
    out_dir = project_root / "data" / "raw" / datetime.now().strftime("%Y-%m-%d")
    out_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(5000)

        # 保存渲染后的 HTML，方便你检查
        rendered_html = page.content()
        (out_dir / "eastmoney_rendered_search.html").write_text(
            rendered_html, encoding="utf-8"
        )

        # 先把所有 a 标签的 href 抽出来看看
        links = page.eval_on_selector_all(
            "a",
            """
            elements => elements.map(a => ({
                text: (a.innerText || "").trim(),
                href: a.href || ""
            }))
            """
        )

        browser.close()

    # 只保留详情页链接
    report_links = []
    seen = set()
    for item in links:
        href = item["href"]
        if "data.eastmoney.com/report/zw_stock.jshtml?infocode=" in href:
            if href not in seen:
                seen.add(href)
                report_links.append(item)

    print(f"[INFO] Total <a> links found: {len(links)}")
    print(f"[INFO] Report detail links found: {len(report_links)}")

    for item in report_links[:20]:
        print(item)


if __name__ == "__main__":
    main()

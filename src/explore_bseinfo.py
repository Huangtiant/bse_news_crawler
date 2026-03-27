"""
探索脚本：分析 bseinfo.net 公告列表页的真实 DOM 结构。
运行后会在 data/raw/<today>/ 保存渲染后的 HTML，供分析 selector 用。

用法：
    cd src
    python explore_bseinfo.py
"""
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright

URL = "https://www.bseinfo.net/disclosure/announcement.html"


def main():
    project_root = Path(__file__).resolve().parent.parent
    out_dir = project_root / "data" / "raw" / datetime.now().strftime("%Y-%m-%d")
    out_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # 有头，方便观察
        page = browser.new_page(viewport={"width": 1280, "height": 900})

        print(f"[INFO] 打开页面: {URL}")
        page.goto(URL, wait_until="domcontentloaded", timeout=60000)

        # 等待列表内容加载（JS 渲染）
        page.wait_for_timeout(5000)

        # 1. 保存完整渲染后的 HTML
        html = page.content()
        html_file = out_dir / "bseinfo_announcement.html"
        html_file.write_text(html, encoding="utf-8")
        print(f"[INFO] HTML 已保存: {html_file}")

        # 2. 打印所有 a 标签，找公告列表条目和 PDF 链接的模式
        links = page.eval_on_selector_all(
            "a",
            "elements => elements.map(a => ({ text: (a.innerText||'').trim(), href: a.href||'' }))"
        )
        print(f"\n[INFO] 共找到 <a> 标签: {len(links)} 个")
        print("[INFO] 包含 'pdf' 或 '.pdf' 的链接：")
        for item in links:
            if "pdf" in item["href"].lower() or "pdf" in item["text"].lower():
                print("  ", item)

        # 3. 打印页面上所有可见文字，快速确认公告是否已渲染
        print("\n[INFO] 页面可见文字（前 3000 字符）：")
        body_text = page.inner_text("body")
        print(body_text[:3000])

        # 4. 等待一下，方便人工观察浏览器窗口
        print("\n[INFO] 浏览器窗口保持 15 秒，请手动观察页面结构...")
        # 用 JS click() 直接触发 span 的事件监听器
        found = page.evaluate("""
            () => {
                const span = document.querySelector('span[code="distype"][title="年度报告"]');
                if (!span) return false;
                span.click();
                return true;
            }
        """)
        print(f"[INFO] 年度报告 span 找到并点击: {found}")
        page.wait_for_timeout(3000)  # 肉眼观察筛选是否高亮

        page.wait_for_timeout(15000)

        browser.close()


if __name__ == "__main__":
    main()

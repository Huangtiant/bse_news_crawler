import re
from typing import List, Dict
from urllib.parse import quote
from playwright.sync_api import sync_playwright


SEARCH_URL_TEMPLATE = "https://so.eastmoney.com/yanbao/s?keyword={keyword}"


def build_search_url(keyword: str) -> str:
    return SEARCH_URL_TEMPLATE.format(keyword=quote(keyword))


def extract_report_links_with_playwright(keyword: str) -> List[Dict[str, str]]:
    url = build_search_url(keyword)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 800})

        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(5000)

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

    results = []
    seen = set()

    for item in links:
        href = item["href"]
        if "data.eastmoney.com/report/zw_stock.jshtml?infocode=" not in href:
            continue

        match = re.search(r"infocode=([A-Z0-9]+)", href)
        if not match:
            continue

        infocode = match.group(1)
        if infocode in seen:
            continue
        seen.add(infocode)

        results.append({
            "title_hint": item["text"],
            "url": href,
            "infocode": infocode,
        })

    return results

from bs4 import BeautifulSoup
from fetcher import fetch_url


def fetch_report_detail(url: str) -> Dict[str, str]:
    html = fetch_url(url)
    soup = BeautifulSoup(html, "lxml")

    title = ""
    publish_time = ""
    content = ""

    if soup.title:
        title = soup.title.get_text(strip=True)

    body_text = soup.get_text("\n", strip=True)

    return {
        "url": url,
        "title": title,
        "publish_time": publish_time,
        "content": body_text[:50000],
        "raw_html": html,
    }

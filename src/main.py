from pathlib import Path
from datetime import datetime
from cleaner import clean_report_record
import json

from storage import save_text
from sources.eastmoney_report import (
    extract_report_links_with_playwright,
    fetch_report_detail,
)


def main():
    project_root = Path(__file__).resolve().parent.parent
    raw_dir = project_root / "data" / "raw"
    parsed_dir = project_root / "data" / "parsed"

    today = datetime.now().strftime("%Y-%m-%d")
    keyword = "北交所"

    # 1. 提取详情页链接
    report_links = extract_report_links_with_playwright(keyword)

    parsed_links_file = parsed_dir / today / "eastmoney_report_urls.json"
    parsed_links_file.parent.mkdir(parents=True, exist_ok=True)
    parsed_links_file.write_text(
        json.dumps(report_links, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"[INFO] Found {len(report_links)} report links")

    # 2. 抓前 5 条详情页测试
    clean_dir = project_root / "data" / "clean"    
    details = []
    for item in report_links[:5]:
        print(f"[INFO] Fetching detail: {item['url']}")
        detail = fetch_report_detail(item["url"])
        detail["infocode"] = item["infocode"]
        detail["title_hint"] = item["title_hint"]

        # 输出清洗后的 txt
        clean_text = clean_report_record(
            title=detail.get("title", ""),
            publish_time=detail.get("publish_time", ""),
            url=detail.get("url", ""),
            html=detail.get("raw_html", ""),
        )
        clean_txt_file = clean_dir / today / f"{item['infocode']}.txt"
        save_text(clean_txt_file, clean_text)

        # 保存原始 HTML
        # raw_html_file = raw_dir / today / "reports" / f"{item['infocode']}.html"
        # save_text(raw_html_file, detail["raw_html"])

        # 去掉 raw_html 再写入结构化结果
        detail_for_json = dict(detail)
        detail_for_json.pop("raw_html", None)
        details.append(detail_for_json)

    parsed_details_file = parsed_dir / today / "eastmoney_report_details.json"
    parsed_details_file.write_text(
        json.dumps(details, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"[INFO] Saved report links to: {parsed_links_file}")
    print(f"[INFO] Saved report details to: {parsed_details_file}")


if __name__ == "__main__":
    main()

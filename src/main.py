from datetime import datetime

from config import EASTMONEY_KEYWORD
from storage import save_text
from storage.path_builder import PathBuilder
from storage.jsonl_store import JsonlStore
from models.record import ContentStatus
from cleaner import clean_report_record
from normalizers import bseinfo_normalizer, eastmoney_normalizer
from sources.eastmoney_report import extract_report_links_with_playwright, fetch_report_detail
from sources.bseinfo_announcement import fetch_all_announcements

_pb    = PathBuilder()
_store = JsonlStore(_pb)


def run_bseinfo_pipeline() -> None:
    """
    板块一：抓取北交所各类公告的 PDF 直链，写入 records JSONL。
    不下载 PDF（留给后续阶段）。记录按 publish_date 自动落入对应 jsonl 文件。
    """
    all_raw = fetch_all_announcements()

    all_records = []
    for type_name, raws in all_raw.items():
        records = bseinfo_normalizer.normalize_many(raws)
        all_records.extend(records)
        print(f"[bseinfo] [{type_name}] {len(records)} 条")

    stats = _store.upsert_records(all_records)
    print(f"[bseinfo] 共处理 {len(all_records)} 条 → 新增 {stats['inserted']}，更新 {stats['updated']}")


def run_eastmoney_pipeline(today: str) -> None:
    """
    板块二：东方财富研报搜索 → 抓详情 → 清洗正文 → 写入 records JSONL。
    输出：data/records/{today}.jsonl
          data/text/eastmoney/{today}/{id}.txt
    """
    report_links = extract_report_links_with_playwright(EASTMONEY_KEYWORD)
    print(f"[eastmoney] 找到 {len(report_links)} 条研报链接")

    for item in report_links[:5]:
        print(f"[eastmoney] 抓取详情: {item['url']}")
        detail = fetch_report_detail(item["url"])

        # 合并链接列表字段 + 详情页字段，交给 normalizer 处理
        merged = {**item, **detail}
        record = eastmoney_normalizer.normalize(merged)

        # 清洗正文，直接在写入前补全 content 字段
        clean_text = clean_report_record(
            title=detail.get("title", ""),
            publish_time=detail.get("publish_time", ""),
            url=detail.get("url", ""),
            html=detail.get("raw_html", ""),
        )
        txt_path = _pb.text_file("eastmoney", record.publish_date, record.id)
        save_text(txt_path, clean_text)

        record.content_text    = clean_text
        record.content_status  = ContentStatus.CLEANED
        record.text_file_path  = _pb.text_file_rel("eastmoney", record.publish_date, record.id)

        _store.upsert_record(record)
        print(f"[eastmoney] 写入: {record.id} / {record.title[:30]}")

    print(f"[eastmoney] 完成 → {_pb.record_file(today)}")


def main():
    today = datetime.now().strftime("%Y-%m-%d")
    run_bseinfo_pipeline()
    run_eastmoney_pipeline(today)


if __name__ == "__main__":
    main()

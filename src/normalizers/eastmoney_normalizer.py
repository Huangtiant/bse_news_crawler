"""
东方财富文章 normalizer。

输入：eastmoney_report.fetch_report_detail() 产出的 dict，
      外加抓取列表时的 infocode / title_hint：
    {
        url, title, publish_time, content, raw_html,   # 来自 fetch_report_detail
        infocode, title_hint                            # 来自链接列表
    }

输出：统一 Record
"""
from models.record import Record, ContentStatus
from storage.path_builder import PathBuilder

_pb = PathBuilder()


def normalize(raw: dict) -> Record:
    """
    将 eastmoney 原始 dict 转换为统一 Record。

    - id：sha256("eastmoney:" + infocode)，infocode 是东方财富的稳定主键
    - publish_date：取 publish_time 的前 10 位（YYYY-MM-DD）；
                    若为空则取 fetch_time 当天（降级处理）
    - title：优先用详情页抓到的 title，回退到搜索列表的 title_hint
    - content_status：有 raw_html 则标记为 downloaded，否则 empty
    """
    infocode     = raw.get("infocode", "")
    record_id    = Record.make_id("eastmoney", infocode)
    publish_date = (raw.get("publish_time") or "")[:10]  # "YYYY-MM-DD HH:MM" → "YYYY-MM-DD"
    fetch_now    = Record.now_iso()

    if not publish_date:
        publish_date = fetch_now[:10]   # UTC 日期作为降级

    title = raw.get("title") or raw.get("title_hint", "")
    has_html = bool(raw.get("raw_html"))

    return Record(
        id=record_id,
        source="eastmoney",
        source_type="media",
        record_type="article",
        title=title,
        category="研报",
        publish_date=publish_date,
        fetch_time=fetch_now,
        url=raw.get("url", ""),
        raw_file_path=_pb.raw_file_rel("eastmoney", publish_date, record_id, ".html") if has_html else "",
        content_status=ContentStatus.DOWNLOADED if has_html else ContentStatus.EMPTY,
        extra={"infocode": infocode, "title_hint": raw.get("title_hint", "")},
    )


def normalize_many(raws: list[dict]) -> list[Record]:
    return [normalize(r) for r in raws]

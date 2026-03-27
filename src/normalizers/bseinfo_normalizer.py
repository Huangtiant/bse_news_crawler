"""
北交所公告 normalizer。

输入：bseinfo_announcement.fetch_all_announcements() 产出的单条 dict：
    {
        company_code, company_name, title,
        pdf_url, date, report_type
    }

输出：统一 Record
"""
from models.record import Record, ContentStatus
from storage.path_builder import PathBuilder

_pb = PathBuilder()


def normalize(raw: dict) -> Record:
    """
    将 bseinfo 原始 dict 转换为统一 Record。

    - id：sha256("bseinfo:" + pdf_url)，pdf_url 含日期和随机 hash，全局唯一
    - publish_date：直接取 raw["date"]，格式已是 YYYY-MM-DD
    - category：取 raw["report_type"]
    - content_status：初始为 empty，PDF 下载后由后续流程更新
    """
    pdf_url      = raw.get("pdf_url", "")
    publish_date = raw.get("date", "")
    record_id    = Record.make_id("bseinfo", pdf_url)

    return Record(
        id=record_id,
        source="bseinfo",
        source_type="official",
        record_type="announcement",
        company_code=raw.get("company_code", ""),
        company_name=raw.get("company_name", ""),
        title=raw.get("title", ""),
        category=raw.get("report_type", ""),
        publish_date=publish_date,
        fetch_time=Record.now_iso(),
        pdf_url=pdf_url,
        raw_file_path=_pb.raw_file_rel("bseinfo", publish_date, record_id, ".pdf"),
        content_status=ContentStatus.EMPTY,
        extra={"report_type": raw.get("report_type", "")},
    )


def normalize_many(raws: list[dict]) -> list[Record]:
    return [normalize(r) for r in raws]

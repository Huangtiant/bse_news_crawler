"""
阶段 1 验证脚本：展示两种来源如何映射到 Record。
运行方式：python src/models/demo_record.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.record import Record, ContentStatus


# ── 模拟 bseinfo_announcement 抓取到的原始 dict ──
raw_bseinfo = {
    "company_code": "920445",
    "company_name": "柔灵科技",
    "title": "[年度报告摘要]柔灵科技:2025年年度缺陷摘要",
    "pdf_url": "https://www.bseinfo.net/disclosure/2026/2026-03-27/9b2dfecd02af4acdaf76a4425051d1ec.pdf",
    "date": "2026-03-27",
    "report_type": "年度报告",
}

bseinfo_record = Record(
    id=Record.make_id("bseinfo", raw_bseinfo["pdf_url"]),
    source="bseinfo",
    source_type="official",
    record_type="announcement",
    company_code=raw_bseinfo["company_code"],
    company_name=raw_bseinfo["company_name"],
    title=raw_bseinfo["title"],
    category=raw_bseinfo["report_type"],
    publish_date=raw_bseinfo["date"],
    fetch_time=Record.now_iso(),
    pdf_url=raw_bseinfo["pdf_url"],
    content_status=ContentStatus.EMPTY,
    extra={"report_type": raw_bseinfo["report_type"]},
)


# ── 模拟 eastmoney_report 抓取到的原始 dict ──
raw_eastmoney = {
    "infocode": "AP202603271820801732",
    "title_hint": "北交所最新消息：某品牌ODM纺织企业2025年营收增21%",
    "url": "http://data.eastmoney.com/report/zw_stock.jshtml?infocode=AP202603271820801732",
    "title": "某品牌ODM纺织企业2025年营收增21%",
    "publish_time": "2026-03-27",
}

eastmoney_record = Record(
    id=Record.make_id("eastmoney", raw_eastmoney["infocode"]),
    source="eastmoney",
    source_type="media",
    record_type="article",
    title=raw_eastmoney.get("title") or raw_eastmoney["title_hint"],
    category="研报",
    publish_date=raw_eastmoney.get("publish_time", ""),
    fetch_time=Record.now_iso(),
    url=raw_eastmoney["url"],
    content_status=ContentStatus.EMPTY,
    extra={"infocode": raw_eastmoney["infocode"]},
)


# ── 验证输出 ──
print("=" * 60)
print("【bseinfo 公告 → Record】")
print(json.dumps(bseinfo_record.to_dict(), ensure_ascii=False, indent=2))

print()
print("=" * 60)
print("【eastmoney 文章 → Record】")
print(json.dumps(eastmoney_record.to_dict(), ensure_ascii=False, indent=2))

print()
print("=" * 60)
print("【序列化为 JSONL 单行】")
print(bseinfo_record.to_json_line())

print()
print("【from_dict 反序列化验证】")
restored = Record.from_dict(bseinfo_record.to_dict())
assert restored.id == bseinfo_record.id
assert restored.company_code == "920445"
print("OK - id 一致，字段正确")

print()
print("【id 稳定性验证：相同输入，两次生成 id 相同】")
id1 = Record.make_id("bseinfo", raw_bseinfo["pdf_url"])
id2 = Record.make_id("bseinfo", raw_bseinfo["pdf_url"])
assert id1 == id2
print(f"OK - id: {id1}")

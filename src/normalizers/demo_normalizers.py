"""
阶段 4 验证脚本：展示两个 normalizer 的输入输出。
运行方式：python src/normalizers/demo_normalizers.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from normalizers import bseinfo_normalizer, eastmoney_normalizer
from models.record import ContentStatus


# ── bseinfo 原始数据（模拟抓取结果）──────────────────────────────────
raw_bseinfo = {
    "company_code": "920445",
    "company_name": "柔灵科技",
    "title": "[年度报告摘要]柔灵科技:2025年年度报告摘要",
    "pdf_url": "https://www.bseinfo.net/disclosure/2026/2026-03-27/9b2dfecd02af4acdaf76a4425051d1ec.pdf",
    "date": "2026-03-27",
    "report_type": "年度报告",
}

r_bseinfo = bseinfo_normalizer.normalize(raw_bseinfo)

print("=" * 60)
print("【bseinfo 原始 dict】")
print(json.dumps(raw_bseinfo, ensure_ascii=False, indent=2))
print()
print("【→ 标准化 Record】")
print(json.dumps(r_bseinfo.to_dict(), ensure_ascii=False, indent=2))


# ── eastmoney 原始数据（模拟抓取结果，有 raw_html）────────────────────
raw_eastmoney_full = {
    "infocode": "AP202603271820801732",
    "title_hint": "某品牌ODM纺织企业2025年营收增21%（简版标题）",
    "url": "http://data.eastmoney.com/report/zw_stock.jshtml?infocode=AP202603271820801732",
    "title": "某品牌ODM纺织企业2025年营收增21%（详情页完整标题）",
    "publish_time": "2026-03-27 08:30",
    "raw_html": "<html>...</html>",   # 有内容，content_status 应为 downloaded
}

r_eastmoney = eastmoney_normalizer.normalize(raw_eastmoney_full)

print()
print("=" * 60)
print("【eastmoney 原始 dict（含 raw_html）】")
display = {k: v for k, v in raw_eastmoney_full.items() if k != "raw_html"}
display["raw_html"] = "(省略)"
print(json.dumps(display, ensure_ascii=False, indent=2))
print()
print("【→ 标准化 Record】")
print(json.dumps(r_eastmoney.to_dict(), ensure_ascii=False, indent=2))


# ── eastmoney 仅有链接（未抓详情页）──────────────────────────────────
raw_eastmoney_stub = {
    "infocode": "AP202603271820801999",
    "title_hint": "另一篇研报标题",
    "url": "http://data.eastmoney.com/report/zw_stock.jshtml?infocode=AP202603271820801999",
}

r_stub = eastmoney_normalizer.normalize(raw_eastmoney_stub)

print()
print("=" * 60)
print("【eastmoney 原始 dict（仅链接，无 raw_html）】")
print(json.dumps(raw_eastmoney_stub, ensure_ascii=False, indent=2))
print()
print("【→ 标准化 Record（content_status 应为 empty）】")
print(f"  title          = {r_stub.title}")
print(f"  content_status = {r_stub.content_status}")
print(f"  raw_file_path  = {r_stub.raw_file_path!r}")


# ── 断言 ──────────────────────────────────────────────────────────────
assert r_bseinfo.source == "bseinfo"
assert r_bseinfo.source_type == "official"
assert r_bseinfo.record_type == "announcement"
assert r_bseinfo.category == "年度报告"
assert r_bseinfo.publish_date == "2026-03-27"
assert r_bseinfo.content_status == ContentStatus.EMPTY
assert r_bseinfo.raw_file_path.endswith(".pdf")
assert r_bseinfo.raw_file_path.startswith("raw/bseinfo") or r_bseinfo.raw_file_path.startswith("raw\\bseinfo")

assert r_eastmoney.source == "eastmoney"
assert r_eastmoney.source_type == "media"
assert r_eastmoney.content_status == ContentStatus.DOWNLOADED   # 有 raw_html
assert r_eastmoney.publish_date == "2026-03-27"
assert r_eastmoney.extra["infocode"] == "AP202603271820801732"
assert "详情页" in r_eastmoney.title   # 优先用详情页 title

assert r_stub.content_status == ContentStatus.EMPTY             # 无 raw_html
assert r_stub.raw_file_path == ""
assert r_stub.title == "另一篇研报标题"                          # 回退到 title_hint

# id 稳定性：相同输入生成相同 id
assert bseinfo_normalizer.normalize(raw_bseinfo).id == r_bseinfo.id

print()
print("All assertions passed OK")

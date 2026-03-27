# BSE News Crawler

北京证券交易所财经信息抓取与整理工具。

抓取两类数据：
- **北交所公告**（bseinfo）：年报、半年报、季报、业绩预告等 PDF 直链
- **东方财富研报**（eastmoney）：与「北交所」相关的研报文章正文

---

## 目录结构

```
src/
  main.py                     入口，运行两条抓取 pipeline
  config.py                   全局常量（路径、关键词）
  cleaner.py                  HTML/文本清洗工具
  fetcher.py                  HTTP 抓取封装
  models/
    record.py                 统一记录模型（Record dataclass）
  storage/
    __init__.py               save_text / save_json 工具函数
    path_builder.py           所有落盘路径的构建
    jsonl_store.py            JSONL 读写（append / upsert）
  normalizers/
    bseinfo_normalizer.py     bseinfo 原始 dict → Record
    eastmoney_normalizer.py   eastmoney 原始 dict → Record
  readers/
    record_reader.py          按日期/来源/公司/分类查询记录
  updaters/
    record_updater.py         按 id 回填 content_text / summary 等字段
  filters/
    announcement_filter.py   从全量公告中筛选有价值记录
  extractors/
    pdf_extractor.py          PDF → 纯文本（pdfplumber）
    pdf_pipeline.py           下载 → 提取 → 清洗 → 落盘 → 写回 record
  sources/
    bseinfo_announcement.py   北交所公告页面抓取（Playwright）
    eastmoney_report.py       东方财富研报抓取（Playwright + requests）

data/
  records/
    2026-03-27.jsonl          所有来源当天记录，每行一个 JSON 对象
  raw/
    bseinfo/{date}/           下载的原始 PDF 文件
    eastmoney/{date}/         抓取的原始 HTML 文件
  text/
    bseinfo/{date}/           从 PDF 提取并清洗的 txt
    eastmoney/{date}/         从 HTML 清洗的 txt
```

---

## 数据模型（Record）

每条记录的核心字段：

| 字段 | 说明 |
|---|---|
| `id` | `sha256(source:unique_key)[:16]`，全局唯一，用于去重和更新 |
| `source` | `bseinfo` / `eastmoney` |
| `source_type` | `official`（交易所）/ `media`（财经媒体）|
| `record_type` | `announcement` / `article` |
| `company_code` | 股票代码（eastmoney 来源可能为空）|
| `category` | 年度报告 / 半年度报告 / 研报 … |
| `publish_date` | `YYYY-MM-DD`，按此字段决定写入哪个 jsonl 文件 |
| `pdf_url` | PDF 直链（bseinfo 公告）|
| `content_status` | 见下表 |
| `content_text` | 提取并清洗后的正文 |
| `summary` | AI 摘要（后续填充）|
| `extra` | 来源特有字段（infocode、report_type 等）|

`content_status` 状态流转：

```
empty → downloaded → extracted → cleaned → summarized
                         ↓           ↓
                       failed      failed
```

---

## 运行方式

**环境准备**

```bash
python -m venv venv
venv/Scripts/activate        # Windows
pip install -r requirements.txt
playwright install chromium
```

**运行抓取**

```bash
cd src
python main.py
```

运行后产出：
- `data/records/{today}.jsonl` — 当天全部记录
- `data/text/eastmoney/{today}/*.txt` — 东方财富文章清洗文本

**单独提取 PDF 文本**（需先运行 `main.py` 抓到记录）

```python
from readers.record_reader import RecordReader
from filters.announcement_filter import filter_announcements
from extractors.pdf_pipeline import process_records

records    = RecordReader().by_date("2026-03-27").fetch()
candidates = filter_announcements(records)
stats      = process_records(candidates)
print(stats)  # {"cleaned": N, "failed": M, "skipped": K}
```

---

## 读取记录

```python
from readers.record_reader import RecordReader

reader = RecordReader()

# 某一天全部记录
records = reader.by_date("2026-03-27").fetch()

# 日期范围 + 来源过滤
records = reader.by_date_range("2026-03-01", "2026-03-27").source("bseinfo").fetch()

# 按公司代码
records = reader.by_date("2026-03-27").company_code("920445").fetch()

# 多条件组合
records = (
    reader.by_date("2026-03-27")
    .source("bseinfo")
    .category("年度报告")
    .content_status("empty")
    .fetch()
)
```

---

## 回填正文（PDF 提取后）

```python
from updaters.record_updater import RecordUpdater

updater = RecordUpdater()

# 回填正文
updater.update_content(
    record_id="11e4337148c470dc",
    publish_date="2026-03-27",
    content_text="提取到的文本...",
    text_file_path="text/bseinfo/2026-03-27/11e433.txt",
)

# 回填摘要
updater.update_summary(
    record_id="11e4337148c470dc",
    publish_date="2026-03-27",
    summary="营收增长18%，业绩稳健。",
    tags=["年度报告", "北交所"],
)
```

---

## 旧数据迁移说明

重构前（旧结构）产出以下文件，现已不再生成：

| 旧路径 | 对应新位置 |
|---|---|
| `data/announcements/{date}/{type}.json` | `data/records/{date}.jsonl`（bseinfo 公告）|
| `data/articles/{date}/urls.json` | `data/records/{date}.jsonl`（eastmoney 链接）|
| `data/articles/{date}/{infocode}.txt` | `data/text/eastmoney/{date}/{id}.txt` |
| `data/parsed/` | 废弃，内容已合并进 records |

旧文件不需要手动删除，新流程不依赖它们，两者互不干扰。

如需将旧 `announcements/*.json` 数据迁入新格式：

```python
import json
from pathlib import Path
from normalizers import bseinfo_normalizer
from storage.jsonl_store import JsonlStore

store = JsonlStore()
for json_file in Path("data/announcements").rglob("*.json"):
    raws = json.loads(json_file.read_text(encoding="utf-8"))
    records = bseinfo_normalizer.normalize_many(raws)
    stats = store.upsert_records(records)
    print(f"{json_file.name}: {stats}")
```

---

## 后续扩展方向

- **SQLite 迁移**：当前 JSONL 方案适合中小数据量。若记录超过 10 万条或需要复杂查询，可将 `JsonlStore` 替换为 SQLite 实现，`RecordReader` 接口不需要改变。
- **新增数据源**：在 `sources/` 添加抓取模块，在 `normalizers/` 添加对应 normalizer，`main.py` 接入新 pipeline 即可。
- **AI 摘要**：提取文本后调用 `RecordUpdater.update_summary()`，摘要结果写回对应 record。

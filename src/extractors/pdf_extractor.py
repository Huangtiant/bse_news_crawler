"""
PDF 文本提取器。

对外只暴露一个函数：
    extract_text(pdf_path) -> str

底层使用 pdfplumber。如果将来需要换成 pymupdf 或 OCR，
只需替换这一个文件，调用方（pipeline / updater）不需要改动。

提取后的处理流程（由调用方完成）：
    1. extract_text(pdf_path)         → raw_text
    2. save_text(txt_path, raw_text)  → 落盘
    3. updater.update_content(...)    → 写回 record
"""
from __future__ import annotations

from pathlib import Path


class PdfExtractionError(Exception):
    """PDF 提取失败时抛出，调用方可据此将 content_status 标记为 failed。"""


def extract_text(pdf_path: str | Path) -> str:
    """
    从 PDF 文件提取纯文本。

    策略：
      - 逐页提取，页间用两个换行分隔
      - 去除空白页
      - 不做清洗（清洗交给 cleaner 层）

    参数：
        pdf_path — PDF 文件的绝对或相对路径

    返回：
        提取到的纯文本字符串（可能很长）

    异常：
        PdfExtractionError — 文件不存在、无法解析、或所有页均为空
    """
    import pdfplumber

    path = Path(pdf_path)
    if not path.exists():
        raise PdfExtractionError(f"文件不存在: {path}")

    try:
        pages_text: list[str] = []
        with pdfplumber.open(path) as pdf:
            if not pdf.pages:
                raise PdfExtractionError(f"PDF 没有页面: {path}")

            for page in pdf.pages:
                text = page.extract_text() or ""
                text = text.strip()
                if text:
                    pages_text.append(text)

    except PdfExtractionError:
        raise
    except Exception as e:
        raise PdfExtractionError(f"解析失败 ({path.name}): {e}") from e

    if not pages_text:
        raise PdfExtractionError(f"未能提取到任何文本（可能是扫描件）: {path}")

    return "\n\n".join(pages_text)

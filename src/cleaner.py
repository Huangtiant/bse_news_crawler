import re
from bs4 import BeautifulSoup, Tag


NOISE_PATTERNS = [
    r"手机查看.*",
    r"扫码查看.*",
    r"APP内免费看.*",
    r"分享到.*",
    r"收藏.*",
    r"责任编辑.*",
    r"风险提示.*",
    r"免责声明.*",
    r"特别声明.*",
    r"打开东方财富APP.*",
]


PDF_MARKER = "查看PDF原文"


def normalize_text(text: str) -> str:
    """基础文本归一化：去空白、去多余空行、去无意义空格。"""
    if not text:
        return ""

    text = text.replace("\xa0", " ").replace("\u3000", " ")

    lines = []
    for line in text.splitlines():
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            lines.append(line)

    cleaned_lines = []
    for line in lines:
        if len(line) == 1 and line not in {"一", "二", "三", "四", "五"}:
            continue
        cleaned_lines.append(line)

    text = "\n".join(cleaned_lines)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


def remove_noise_lines(text: str) -> str:
    """移除常见噪音行。"""
    if not text:
        return ""

    kept = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        hit_noise = False
        for pattern in NOISE_PATTERNS:
            if re.search(pattern, line, flags=re.IGNORECASE):
                hit_noise = True
                break

        if not hit_noise:
            kept.append(line)

    return "\n".join(kept).strip()


def extract_main_text_from_html(html: str) -> str:
    """
    从 HTML 中提取较干净的正文文本。
    当前策略：
    1. 去 script/style/noscript/svg 等标签
    2. 优先尝试常见正文容器
    3. 找不到就回退到 body 全文
    """
    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "noscript", "svg", "iframe", "footer", "header"]):
        tag.decompose()

    candidate_selectors = [
        '[class*="article"]',
        '[class*="content"]',
        '[class*="main"]',
        '[class*="report"]',
        '[id*="article"]',
        '[id*="content"]',
        '[id*="main"]',
        '[id*="report"]',
    ]

    best_text = ""

    for selector in candidate_selectors:
        nodes = soup.select(selector)
        for node in nodes:
            if not isinstance(node, Tag):
                continue

            text = node.get_text("\n", strip=True)
            text = normalize_text(text)

            if len(text) > len(best_text):
                best_text = text

    if not best_text:
        body = soup.body if soup.body else soup
        best_text = normalize_text(body.get_text("\n", strip=True))

    best_text = remove_noise_lines(best_text)
    best_text = normalize_text(best_text)

    return best_text


def extract_between_pdf_markers(text: str, marker: str = PDF_MARKER) -> str:
    """
    只保留两个“查看PDF原文”之间的内容。

    规则：
    1. 出现 >= 2 次：保留第一次和第二次之间的内容
    2. 出现 1 次：保留该 marker 后面的内容
    3. 出现 0 次：返回原文
    """
    if not text:
        return ""

    positions = [m.start() for m in re.finditer(re.escape(marker), text)]

    if len(positions) >= 2:
        start = positions[0] + len(marker)
        end = positions[1]
        clipped = text[start:end].strip()
        return normalize_text(clipped)

    if len(positions) == 1:
        start = positions[0] + len(marker)
        clipped = text[start:].strip()
        return normalize_text(clipped)

    return text


def clean_report_record(
    title: str,
    publish_time: str,
    url: str,
    html: str,
) -> str:
    """
    输入详情页信息，输出最终给 OpenClaw/总结模型用的纯文本。
    现在会优先只保留两个“查看PDF原文”之间的内容。
    """
    title = normalize_text(title)
    publish_time = normalize_text(publish_time)

    content = extract_main_text_from_html(html)
    content = extract_between_pdf_markers(content, marker=PDF_MARKER)
    content = remove_noise_lines(content)
    content = normalize_text(content)

    parts = []

    if title:
        parts.append(f"标题：{title}")

    if publish_time:
        parts.append(f"日期：{publish_time}")

    if url:
        parts.append(f"URL：{url}")

    parts.append("")
    parts.append("正文：")
    parts.append(content if content else "（未提取到正文）")

    final_text = "\n".join(parts).strip()
    final_text = re.sub(r"\n{3,}", "\n\n", final_text)

    return final_text

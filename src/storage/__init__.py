"""
storage 包的公共入口。

从这里导入的函数与旧 storage.py 保持兼容：
    from storage import save_text, save_json
"""
import json
from pathlib import Path


def save_text(file_path: Path, content: str) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content, encoding="utf-8")


def save_json(file_path: Path, data) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

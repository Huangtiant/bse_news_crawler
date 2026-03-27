"""
路径构建器。

所有路径均相对于 DATA_ROOT（data/ 目录），
方便存入 Record.raw_file_path / text_file_path，
也方便在任意机器上还原绝对路径。
"""
from pathlib import Path

from config import DATA_ROOT


class PathBuilder:
    """
    集中管理所有落盘路径的构建，不做实际 IO。

    使用示例：
        pb = PathBuilder()
        abs_path  = pb.record_file("2026-03-27")
        rel_str   = pb.raw_file_rel("bseinfo", "2026-03-27", "11e433", suffix=".pdf")
        abs_path2 = pb.to_abs(rel_str)
    """

    def __init__(self, data_root: Path = DATA_ROOT):
        self._root = data_root

    # ── records ──────────────────────────────────────────────────────

    def record_file(self, date: str) -> Path:
        """
        data/records/{date}.jsonl
        e.g. data/records/2026-03-27.jsonl
        """
        return self._root / "records" / f"{date}.jsonl"

    # ── raw ──────────────────────────────────────────────────────────

    def raw_file(self, source: str, date: str, record_id: str, suffix: str) -> Path:
        """
        data/raw/{source}/{date}/{record_id}{suffix}
        e.g. data/raw/bseinfo/2026-03-27/11e4337148c470dc.pdf
        """
        return self._root / "raw" / source / date / f"{record_id}{suffix}"

    def raw_file_rel(self, source: str, date: str, record_id: str, suffix: str) -> str:
        """返回相对于 DATA_ROOT 的字符串路径，用于存入 Record.raw_file_path。"""
        return str(Path("raw") / source / date / f"{record_id}{suffix}")

    # ── text ─────────────────────────────────────────────────────────

    def text_file(self, source: str, date: str, record_id: str) -> Path:
        """
        data/text/{source}/{date}/{record_id}.txt
        e.g. data/text/eastmoney/2026-03-27/0674833678a05dea.txt
        """
        return self._root / "text" / source / date / f"{record_id}.txt"

    def text_file_rel(self, source: str, date: str, record_id: str) -> str:
        """返回相对于 DATA_ROOT 的字符串路径，用于存入 Record.text_file_path。"""
        return str(Path("text") / source / date / f"{record_id}.txt")

    # ── 工具 ─────────────────────────────────────────────────────────

    def to_abs(self, rel_path: str) -> Path:
        """将存在 Record 里的相对路径还原为绝对路径。"""
        return self._root / rel_path

"""
阶段 2 验证脚本：展示 PathBuilder 各方法的输入输出。
运行方式：python src/storage/demo_path_builder.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from storage.path_builder import PathBuilder

pb = PathBuilder()

cases = [
    ("record_file",   pb.record_file("2026-03-27")),
    ("record_file",   pb.record_file("2026-03-28")),
    ("raw_file pdf",  pb.raw_file("bseinfo",   "2026-03-27", "11e4337148c470dc", ".pdf")),
    ("raw_file html", pb.raw_file("eastmoney", "2026-03-27", "0674833678a05dea", ".html")),
    ("text_file",     pb.text_file("bseinfo",   "2026-03-27", "11e4337148c470dc")),
    ("text_file",     pb.text_file("eastmoney", "2026-03-27", "0674833678a05dea")),
]

print("── 绝对路径 ──────────────────────────────────────────")
for label, p in cases:
    print(f"  [{label}]\n    {p}\n")

print("── 相对路径（存入 Record 字段）────────────────────────")
rel_raw  = pb.raw_file_rel("bseinfo", "2026-03-27", "11e4337148c470dc", ".pdf")
rel_text = pb.text_file_rel("eastmoney", "2026-03-27", "0674833678a05dea")
print(f"  raw_file_path  = {rel_raw!r}")
print(f"  text_file_path = {rel_text!r}")

print()
print("── to_abs 还原验证 ────────────────────────────────────")
restored = pb.to_abs(rel_raw)
expected = pb.raw_file("bseinfo", "2026-03-27", "11e4337148c470dc", ".pdf")
assert restored == expected, f"mismatch: {restored} != {expected}"
print(f"  OK - {restored}")

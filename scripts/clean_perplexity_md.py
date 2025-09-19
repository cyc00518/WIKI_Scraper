#!/usr/bin/env python3
"""
清理 perplexity 資料夾內的 Markdown 檔：
- 移除 Markdown 強調標記：**bold**, *italic*, __bold__, _italic_
- 移除行首引用符號：> 
- 保持其他文字不變
- 處理後的檔案會存到 perplexity/cleaned/ (保留原檔名)

用法: python scripts/clean_perplexity_md.py
"""
from pathlib import Path
import re

SRC_DIR = Path(__file__).resolve().parents[1] / 'perplexity'
OUT_DIR = SRC_DIR / 'cleaned'
OUT_DIR.mkdir(exist_ok=True)

md_files = sorted([p for p in SRC_DIR.glob('*.md') if p.is_file()])
if not md_files:
    print('No .md files found in', SRC_DIR)
    raise SystemExit(0)

# Regex patterns
PAT_BOLD_DOUBLE_ASTER = re.compile(r"\*\*(.+?)\*\*", flags=re.S)
PAT_BOLD_DOUBLE_UNDER = re.compile(r"__(.+?)__", flags=re.S)
# single asterisk/underscore emphasis - fairly conservative: match a pair surrounding some non-empty content
PAT_ASTER = re.compile(r"(?<!\*)\*(?!\s)(.+?)(?<!\s)\*(?!\*)", flags=re.S)
PAT_UNDER = re.compile(r"(?<!_)_(?!\s)(.+?)(?<!\s)_(?!_)", flags=re.S)

for p in md_files:
    text = p.read_text(encoding='utf-8')
    original = text

    # 0) 先移除 HTML 標籤（整個 <...>）以及 http(s) 或 www 開頭的外部網址
    # 這會去掉 <img ...> 等元素，也會清除內文中的裸連結
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"www\.\S+", "", text)

    # 1) 移除行首的引用符號（>）但保留原本的縮排與內容
    lines = []
    for line in text.splitlines():
        new = re.sub(r"^[ \t]*>[ \t]?", "", line)
        lines.append(new)
    text = "\n".join(lines)

    # 2) 移除強調語法（順序：雙標記 -> 單標記），保留內容
    text = PAT_BOLD_DOUBLE_ASTER.sub(r"\1", text)
    text = PAT_BOLD_DOUBLE_UNDER.sub(r"\1", text)
    text = PAT_ASTER.sub(r"\1", text)
    text = PAT_UNDER.sub(r"\1", text)

    # 2.5) 合併段落內硬換行（保留標題、清單、表格、程式區塊等結構）
    # 將以空行分隔的區塊視為一個段落，若該區塊不是標題/清單/表格/程式碼
    # 則把區塊內的換行轉成空白，合併為單行。
    def unwrap_paragraphs(md_text: str) -> str:
        blocks = re.split(r"\n\s*\n", md_text)
        out_blocks = []
        for blk in blocks:
            lines = blk.splitlines()
            if not lines:
                out_blocks.append("")
                continue
            first = lines[0].lstrip()
            # 判斷是否需要保留原有換行：
            # - 標題（# ...）
            # - 無序/有序清單（- * + • 或數字.)
            # - 表格行以 | 開頭
            # - 程式碼區塊 fence ```
            # - HTML 標籤行（<tag)
            if re.match(r"^#{1,6}\s+", first) or \
               re.match(r"^([\-\*\+\u2022]|\d+\.)\s+", first) or \
               first.startswith("|") or \
               first.startswith("<") or \
               first.startswith("```"):
                # 保留原有換行，但去掉每行前後多餘空白
                cleaned_lines = [ln.rstrip() for ln in lines]
                out_blocks.append("\n".join(cleaned_lines))
            else:
                # 合併段落內所有行為單一行（移除行首尾空白）
                merged = " ".join(ln.strip() for ln in lines if ln.strip())
                out_blocks.append(merged)
        return "\n\n".join(out_blocks)

    text = unwrap_paragraphs(text)

    # 2.6) 移除 footnote 標記像是 [^1], [^note]
    text = re.sub(r"\[\^[^\]]+\]", "", text)

    # 2.7) 刪除刪掉網址後可能只剩下的孤立冒號或冒號+空白的行
    lines = [ln for ln in text.splitlines() if not re.match(r"^\s*:\s*$", ln)]
    text = "\n".join(lines)

    # 2.8) 去除檔案開頭與結尾多餘的空白行
    text = text.strip('\n') + '\n'

    # 3) 寫出到 cleaned 目錄，保留原始檔名
    out_path = OUT_DIR / p.name
    out_path.write_text(text, encoding='utf-8')
    changed = (text != original)
    print(f"{p.name} -> {out_path} (changed={changed})")

print('Done. Processed', len(md_files), 'files.')

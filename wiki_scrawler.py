#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
批次抓取中文維基百科條目（以「臺灣正體 zh-TW」變體輸出）
- 目標清單可為 .txt（每行一個 URL 或標題）或 .jsonl（含 {title|url}）
- 不使用 OpenCC；完全依 MediaWiki 語言變體（Accept-Language / variant）取得內容
- 已存在輸出自動略過，可斷點續抓
- 支援排除指定章節（預設：參考資料,外部連結,相關條目,擴展閱讀,延伸閱讀,參見）
- 段落化輸出：H2 上下各留 1 空行；其餘單行相接；清單項目用「• 」
- 自動修補「<標籤>：<值>」缺值（英語/學名/藝名/本名/原名/別名…），段內 DOM 擷取 + langlinks(en/ja/.../la) 兜底
- 自動移除 CJK 與標點周邊多餘空白，保留英文單字內空白
"""

import argparse
import json
import re
import time
import hashlib
import os
from pathlib import Path
from typing import Iterable, Tuple, Optional
from urllib.parse import urlparse, unquote, quote

import requests
from bs4 import BeautifulSoup, Tag, NavigableString

API = "https://zh.wikipedia.org/w/api.php"
REST_HTML = "https://zh.wikipedia.org/api/rest_v1/page/html/{title}"

DEFAULT_UA = "YourBotName/1.0 (contact@example.com)"  # 建議換成你的資訊


# -------------------------------
# 讀取目標清單
# -------------------------------
def iter_targets(path: Path) -> Iterable[Tuple[str, str, str]]:
    """
    讀取 .txt 或 .jsonl 目標清單，或處理整個資料夾中的所有 .txt 檔案。
    產出 (raw, kind, source_file)：kind = "url" 或 "title"，source_file 是來源檔案路徑
    """
    if path.is_dir():
        # 如果是資料夾，掃描其中所有的 .txt 檔案
        txt_files = list(path.glob("*.txt"))
        jsonl_files = list(path.glob("*.jsonl"))
        all_files = txt_files + jsonl_files
        
        if not all_files:
            print(f"⚠️  資料夾 {path} 中沒有找到 .txt 或 .jsonl 檔案")
            return
        
        print(f"📁 找到 {len(all_files)} 個檔案：{len(txt_files)} 個 .txt 檔案，{len(jsonl_files)} 個 .jsonl 檔案")
        
        for file_path in all_files:
            print(f"📄 處理檔案：{file_path.name}")
            yield from _process_single_file(file_path)
    else:
        # 如果是單一檔案
        yield from _process_single_file(path)


def _process_single_file(file_path: Path) -> Iterable[Tuple[str, str, str]]:
    """
    處理單一檔案，產出 (raw, kind, source_file)
    """
    source_file = str(file_path)
    
    if file_path.suffix.lower() == ".jsonl":
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                    if "url" in obj and obj["url"]:
                        yield obj["url"], "url", source_file
                    elif "title" in obj and obj["title"]:
                        yield obj["title"], "title", source_file
                except json.JSONDecodeError as e:
                    print(f"⚠️  JSON 解析錯誤在 {file_path.name}: {e}")
                    continue
    else:
        with file_path.open("r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                try:
                    if s.startswith("http://") or s.startswith("https://"):
                        yield s, "url", source_file
                    else:
                        yield s, "title", source_file
                except Exception as e:
                    print(f"⚠️  處理行 {line_num} 時出錯在 {file_path.name}: {e}")
                    continue


# -------------------------------
# 工具函數
# -------------------------------
def url_to_title(url: str) -> str:
    path = urlparse(url).path
    title = unquote(path.rsplit("/", 1)[-1])
    return title

def safe_filename(title: str) -> str:
    name = re.sub(r'[\\/*?:"<>|]', "_", title)
    return name[:200]


def clean_display_title(title_html: str) -> str:
    if not title_html:
        return ""
    return BeautifulSoup(title_html, "html.parser").get_text(" ", strip=True)


def extract_display_title_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    meta = soup.find("meta", attrs={"property": "mw:displaytitle"})
    if meta and meta.get("content"):
        return meta["content"].strip()
    heading = soup.select_one("#firstHeading")
    if heading:
        return heading.get_text(" ", strip=True)
    title_tag = soup.find("title")
    if title_tag:
        return title_tag.get_text(" ", strip=True)
    return ""

def http_get_with_backoff(session: requests.Session, url: str, *, params=None,
                          timeout: int = 30, retries: int = 3, backoff_base: float = 2.0):
    last_exc: Optional[Exception] = None
    for i in range(retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 503):
                raise RuntimeError(f"HTTP {r.status_code}")
            # Parsoid / API 也可能以字串提示 maxlag，保守處理
            if r.status_code == 200 and "maxlag" in r.text.lower() and "error" in r.text.lower():
                raise RuntimeError("Server under high replication lag (maxlag).")
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            if i == retries:
                break
            time.sleep(backoff_base ** i)  # 2,4,8...
    raise RuntimeError(f"GET failed for {url}: {last_exc}") from last_exc


# -------------------------------
# 抓取（REST 優先，Action API 後備）
# -------------------------------
def fetch_html_rest(title: str, session: requests.Session, timeout=30) -> tuple[str, str]:
    url = REST_HTML.format(title=quote(title, safe=""))
    r = http_get_with_backoff(session, url, timeout=timeout)
    html = r.text
    display_title = extract_display_title_from_html(html)
    return html, (display_title or title)

def detect_redirect(text: str) -> Optional[str]:
    """
    檢測重定向頁面，返回重定向的目標標題
    """
    # 檢測重定向標記
    redirect_patterns = [
        r"重定向到：\s*•\s*([^\n•]+)",
        r"重新導向至：\s*•\s*([^\n•]+)", 
        r"#REDIRECT\s*\[\[([^\]]+)\]\]",
        r"#重定向\s*\[\[([^\]]+)\]\]"
    ]
    
    for pattern in redirect_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            target = match.group(1).strip()
            # 清理可能的額外標記
            target = re.sub(r'\|.*$', '', target)  # 移除管道連結後的部分
            return target
    
    return None


def detect_redirect_from_html(html: str) -> Optional[str]:
    """
    從HTML結構檢測重定向，特別是處理REST API返回的重定向頁面
    """
    from bs4 import BeautifulSoup
    from urllib.parse import unquote
    
    # 檢查是否為特殊重定向頁面
    if 'Special:Redirect' in html:
        soup = BeautifulSoup(html, 'html.parser')
        
        # 檢查 rel="dc:isVersionOf" 連結
        version_link = soup.find('link', rel='dc:isVersionOf')
        if version_link and version_link.get('href'):
            href = version_link['href']
            # 提取頁面標題 (移除 //zh.wikipedia.org/wiki/ 前綴)
            if '/wiki/' in href:
                encoded_title = href.split('/wiki/')[-1]
                title = unquote(encoded_title)
                return title
        
        # 檢查頁面標題
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text().strip()
            # 移除 " - 维基百科" 等後綴
            title_text = re.sub(r'\s*[-–]\s*[^-–]*维基百科[^-–]*$', '', title_text)
            if title_text and title_text != '重定向':
                return title_text
    
    return None


def fetch_html_action(title: str, session: requests.Session, timeout=30) -> Tuple[str, str]:
    """
    抓取頁面內容，同時返回實際的標題（處理重定向）
    返回: (html_content, actual_title)
    """
    params = {
        "action": "parse",
        "page": title,
        "prop": "text|displaytitle",
        "format": "json",
        "variant": "zh-tw",  # 指定臺灣正體變體
        "maxlag": 5,
    }
    r = http_get_with_backoff(session, API, params=params, timeout=timeout)
    data = r.json()
    if "parse" not in data or "text" not in data["parse"]:
        raise RuntimeError(f"Action parse missing content for: {title}")
    
    # 獲取實際的頁面標題（處理重定向）
    display_title = clean_display_title(data["parse"].get("displaytitle"))
    actual_title = display_title or data["parse"].get("title", title)
    html_content = data["parse"]["text"]["*"]
    
    return html_content, actual_title


# -------------------------------
# 排版整理（Tidy）
# -------------------------------
def zh_tidy(text: str) -> str:
    """
    中英混排空白與標點整理（保留英文單詞內空白；只修剪 CJK 與標點附近）：
    - 書名號/內書名號/引號/括號「內側」去空白：《 作品 》→《作品》、〈 日不落 〉→〈日不落〉、（ 英語：Jolin Tsai ）→（英語：Jolin Tsai）
    - 標點「前」去空白：…… Jolin Tsai ，→ …… Jolin Tsai，
    - 日期「年/月/日」去空白：1980年 9月 15日 → 1980年9月15日
    - 破折號/兩字破折號周邊去空白： — → —、 —— → ——
    - CJK 與 CJK 之間的多餘空白去除；「、」周邊去空白
    - 壓掉 3 連以上空白行
    """
    # 0) 標準化不可見空白
    text = re.sub(r"[ \t\u00A0]+", " ", text)

    # 0.5) 清除數學公式 LaTeX 格式
    # 清除 {\displaystyle ...} 格式（包括嵌套的大括號）
    text = re.sub(r'\{\\displaystyle[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', '', text)
    # 清除其他常見 LaTeX 格式
    text = re.sub(r'\{\\[a-zA-Z]+[^}]*\}', '', text)
    # 清除單獨的 {\displaystyle (沒有結束的情況)
    text = re.sub(r'\{\\displaystyle.*?(?=\n|$)', '', text)
    # 清除更複雜的 LaTeX 數學表達式
    # 清除 \begin{...} ... \end{...} 結構
    text = re.sub(r'\\begin\{[^}]+\}.*?\\end\{[^}]+\}', '', text, flags=re.DOTALL)
    # 清除 LaTeX 命令
    text = re.sub(r'\\[a-zA-Z]+\{[^}]*\}', '', text)
    text = re.sub(r'\\[a-zA-Z]+', '', text)
    # 清除殘留的大括號和數學符號
    text = re.sub(r'\{[^{}]*\}', '', text)
    # 清除連續的特殊字符
    text = re.sub(r'[{}\\^_]+', '', text)

    # 1) 括號/書名號/引號 內側空白
    pairs = [
        ("《", "》"), ("〈", "〉"),
        ("「", "」"), ("『", "』"),
        ("（", "）")
    ]
    for l, r in pairs:
        text = re.sub(fr"{re.escape(l)}\s*(.*?)\s*{re.escape(r)}", fr"{l}\1{r}", text, flags=re.S)

    # 2) 標點「前」不留空白（中文標點）
    text = re.sub(r"\s+([，。、；：！？》）」』])", r"\1", text)

    # 3) 日期「年/月/日」之間不留空白
    text = re.sub(r"(\d{1,4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", r"\1年\2月\3日", text)
    text = re.sub(r"(\d{1,4})\s*年\s*(\d{1,2})\s*月", r"\1年\2月", text)

    # 4) 破折號周邊不留空白（— 與 ——）
    text = re.sub(r"\s*——\s*", "——", text)
    text = re.sub(r"\s*—\s*", "—", text)

    # 5) CJK 與 CJK 之間多餘空白去除；「、」周邊去空白
    CJK = r"\u4E00-\u9FFF\u3400-\u4DBF"
    # 智能處理：保留標題間的換行，但去除段落內的多餘空白
    # 先處理標題行（短行且獨立）
    lines = text.split('\n')
    for i, line in enumerate(lines):
        stripped = line.strip()
        # 如果是短行（可能是標題），保持單獨成行
        if stripped and len(stripped) < 30 and not any(char in stripped for char in '。，！？'):
            continue
        # 對於長段落，去除中文字符間的空格
        if stripped:
            lines[i] = re.sub(fr"(?<=[{CJK}])[ \t\u00A0]+(?=[{CJK}])", "", stripped)
    
    text = '\n'.join(lines)
    text = re.sub(r"\s*、\s*", "、", text)

    # 6) 連續 3 行以上空白 → 2 行（最多只允許兩個換行，用於大標分隔）
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text


# -------------------------------
# 文字抽取（REST 與 Action 皆支援）
# -------------------------------
def smart_text(node: Tag) -> str:
    """
    逐文字節點串接：
    - 只有「上一個字元」與「當前片段第一個字元」都為 ASCII 字母/數字時，才補 1 個空白
    - 其他情況直接相連，避免在 CJK 邊界製造多餘空白
    - 特殊處理有序列表 (ol) 和無序列表 (ul)
    """
    # 如果節點本身是列表，則特殊處理
    if node.name == "ol":
        return process_ordered_list(node)
    elif node.name == "ul":
        return process_unordered_list(node)
    
    # 先收集所有非列表內容
    text_parts = []
    last_ascii = False
    
    # 處理所有直接子節點
    for child in node.children:
        if isinstance(child, NavigableString):
            s = str(child).strip()
            if s:
                cur_ascii = bool(s and s[0].isascii())
                if text_parts and last_ascii and cur_ascii:
                    text_parts.append(" ")
                text_parts.append(s)
                last_ascii = bool(s and s[-1].isascii())
        elif isinstance(child, Tag):
            if child.name in ["ol", "ul"]:
                # 處理列表
                if child.name == "ol":
                    list_content = process_ordered_list(child)
                else:
                    list_content = process_unordered_list(child)
                if list_content:
                    text_parts.append(list_content)
                    last_ascii = False
            elif child.name == "br":
                text_parts.append(" ")
                last_ascii = False
            else:
                # 遞歸處理其他標籤
                child_text = smart_text(child)
                if child_text:
                    cur_ascii = bool(child_text and child_text[0].isascii())
                    if text_parts and last_ascii and cur_ascii:
                        text_parts.append(" ")
                    text_parts.append(child_text)
                    last_ascii = bool(child_text and child_text[-1].isascii())
    
    return "".join(text_parts)


def process_ordered_list(ol_tag: Tag) -> str:
    """處理有序列表，添加序號"""
    items = []
    lis = ol_tag.find_all("li", recursive=False)
    for i, li in enumerate(lis, 1):
        if isinstance(li, Tag):
            li_text = extract_text_from_li(li)
            if li_text:
                items.append(f"{i}. {li_text}")
    return " ".join(items)


def process_unordered_list(ul_tag: Tag) -> str:
    """處理無序列表，添加項目符號"""
    items = []
    for li in ul_tag.find_all("li", recursive=False):
        li_text = extract_text_from_li(li)
        if li_text:
            items.append(f"• {li_text}")
    return " ".join(items)


def extract_text_from_li(li_tag: Tag) -> str:
    """從 li 標籤中提取文字，避免遞歸處理嵌套列表"""
    parts = []
    last_ascii = False
    
    for d in li_tag.descendants:
        if isinstance(d, NavigableString):
            # 跳過嵌套列表中的文字
            parent_li = d.find_parent("li")
            if parent_li and parent_li != li_tag:
                continue
                
            s = str(d)
            if not s or not s.strip():
                continue
            s = s.strip()
            cur_ascii = bool(s and s[0].isascii())
            if parts and last_ascii and cur_ascii:
                parts.append(" ")
            parts.append(s)
            last_ascii = bool(s and s[-1].isascii())
        elif isinstance(d, Tag) and d.name == "br":
            parts.append(" ")
            last_ascii = False
    
    return "".join(parts)


def _parse_int(value, default: int = 1) -> int:
    try:
        return max(int(value), 1)
    except (TypeError, ValueError):
        return default


def table_to_lines(table: Tag, squeeze) -> list[str]:
    """Flatten a wiki table into pipe-separated rows with colspan/rowspan expansion."""
    lines: list[str] = []

    caption = table.find("caption")
    if caption:
        caption_text = squeeze(smart_text(caption))
        if caption_text:
            lines.append(caption_text)

    def table_squeeze(s: str) -> str:
        """表格專用的文本壓縮，保留換行符但轉換為空格"""
        # 將換行符替換為空格，保持在同一個單元格內
        s = s.replace('\n', ' ')
        # 然後進行正常的空白壓縮
        return re.sub(r"[ \t\u00A0]+", " ", s.strip())

    row_spans: list[dict | None] = []
    rows: list[list[str]] = []

    for tr in table.find_all("tr"):
        if tr.find_parent("table") is not table:
            continue

        current_row: list[str | None] = []

        # apply active rowspans to this row
        for idx in range(len(row_spans)):
            span = row_spans[idx]
            if span:
                current_row.append(span["value"])
                span["rows_left"] -= 1
                if span["rows_left"] == 0:
                    row_spans[idx] = None
            else:
                current_row.append(None)

        cells = tr.find_all(["th", "td"], recursive=False)
        for cell in cells:
            # 調試：檢查是否有 ol 標籤
            if isinstance(cell, Tag) and cell.find("ol"):
                ol_tags = cell.find_all("ol")
                for i, ol in enumerate(ol_tags):
                    if isinstance(ol, Tag):
                        processed_ol = process_ordered_list(ol)
            
            if isinstance(cell, Tag):
                text = table_squeeze(smart_text(cell))
                colspan = _parse_int(cell.get("colspan"), 1)
                rowspan = _parse_int(cell.get("rowspan"), 1)
            else:
                continue

            # find the first slot that can host this cell (respecting existing rowspans)
            col_idx = 0
            while True:
                # extend row/rowspan buffers when needed
                while col_idx >= len(current_row):
                    current_row.append(None)
                    row_spans.append(None)

                # ensure the span fits starting at col_idx
                fits = True
                for offset in range(colspan):
                    pos = col_idx + offset
                    while pos >= len(current_row):
                        current_row.append(None)
                        row_spans.append(None)
                    if current_row[pos] is not None:
                        col_idx = pos + 1
                        fits = False
                        break
                if fits:
                    break

            # place the cell content across the colspan
            for offset in range(colspan):
                pos = col_idx + offset
                value = text if offset == 0 else ""
                current_row[pos] = value
                if rowspan > 1:
                    row_spans[pos] = {"rows_left": rowspan - 1, "value": value}
                else:
                    if row_spans[pos] is None:
                        continue
                    row_spans[pos] = None

        cleaned = [c if c is not None else "" for c in current_row]
        while cleaned and not cleaned[-1].strip():
            cleaned.pop()
        if any(col.strip() for col in cleaned):
            rows.append(cleaned)

    for row in rows:
        line = " | ".join(row)
        if line.strip():
            lines.append("• " + line)

    return lines


def process_multicol_table(table: Tag, squeeze) -> list[str]:
    """
    處理 multicol 表格，這種表格通常包含多欄內容，每欄有標題和列表
    例如：音樂作品欄位，包含 "錄音室專輯"、"精選輯"、"日語作品" 等分類
    """
    lines: list[str] = []
    
    # 查找所有的 td 欄位，按順序處理每個欄位
    for td in table.find_all("td"):
        # 按文檔順序處理該欄位中的所有子元素
        for element in td.find_all(["h3", "h4", "h5", "h6", "dl", "ul", "ol"], recursive=True):
            
            # 處理標題（h3, h4 等）
            if element.name in ["h3", "h4", "h5", "h6"]:
                title = squeeze(smart_text(element))
                if title:
                    # 添加子標題（前面有空行分隔）
                    if lines and lines[-1] != "":
                        lines.append("")
                    lines.append(title)
            
            # 處理 dl 標題
            elif element.name == "dl":
                for dt in element.find_all("dt"):
                    title = squeeze(smart_text(dt))
                    if title:
                        # 添加子標題（前面有空行分隔）
                        if lines and lines[-1] != "":
                            lines.append("")
                        lines.append(f"### {title}")
            
            # 處理無序列表 ul
            elif element.name == "ul":
                # 只處理直接子項目，避免重複處理嵌套列表
                for li in element.find_all("li", recursive=False):
                    item = squeeze(smart_text(li))
                    if item:
                        lines.append(f"• {item}")
            
            # 處理有序列表 ol
            elif element.name == "ol":
                # 只處理直接子項目，避免重複處理嵌套列表
                for li in element.find_all("li", recursive=False):
                    item = squeeze(smart_text(li))
                    if item:
                        lines.append(f"• {item}")
    
    return lines


# ===== 通用標籤處理（學名 / 藝名 / 本名 / 原名 / 別名 / 多語言） =====

# 常見標籤 → BCP-47 語言碼（可自行擴充）
LABEL_LANG_MAP = {
    # 語言名稱（含常見別稱）
    "英語": "en", "英文": "en", "English": "en",
    "日語": "ja", "日文": "ja", "Japanese": "ja",
    "韓語": "ko", "韓文": "ko",
    "法語": "fr", "法文": "fr",
    "德語": "de", "德文": "de",
    "西班牙語": "es", "西文": "es", "西語": "es",
    "俄語": "ru", "俄文": "ru",
    "義大利語": "it", "義文": "it", "意大利語": "it", "意文": "it",
    "葡萄牙語": "pt", "葡文": "pt",
    "拉丁語": "la", "拉丁文": "la",
    "越南語": "vi", "越文": "vi",
    "泰語": "th",
    "馬來語": "ms", "馬來文": "ms",
    "印尼語": "id", "印尼文": "id",
    "粵語": "yue", "廣東話": "yue",
    "閩南語": "nan", "臺語": "nan", "台語": "nan", "閩南話": "nan",
    "客語": "hak",
    # 非語言但常見：學名（多半拉丁）
    "學名": "la",
}

# 會被當成「值缺失」的分隔符（遇到它們就停止蒐集）
_STOP_CHARS = "，,、；;,)）」』】〉。."

def _fetch_langlink_title(title: str, session: requests.Session, lang_code: str) -> Optional[str]:
    """
    用 MediaWiki Action API 讀取互語連結標題（lllang=<code>）
    """
    params = {
        "action": "query",
        "prop": "langlinks",
        "titles": title,
        "lllang": lang_code,
        "format": "json",
    }
    try:
        r = http_get_with_backoff(session, API, params=params, timeout=20)
        data = r.json()
        pages = data.get("query", {}).get("pages", {})
        for _, page in pages.items():
            for ll in page.get("langlinks", []) or []:
                if ll.get("lang") == lang_code:
                    return ll.get("*") or ll.get("title")
    except Exception:
        pass
    return None


def _collect_value_after_label(el: Tag, label: str, maxlen: int = 120) -> Optional[str]:
    """
    在段落 el 裡找到「<label>：」這個邏輯序列（允許 label 與冒號跨節點），
    之後把值一路收集到遇到停止字元為止。
    例：<a>學名</a>：<i><span lang="la">Oncorhynchus masou formosanus</span></i>
    """
    # 1) 找到文字剛好等於 label 的節點（可能是 NavigableString 或 <a>/<b>/<span> 等 Tag）
    anchor: Optional[object] = None
    for d in el.descendants:
        if isinstance(d, NavigableString):
            if d.strip() == label:
                anchor = d
                break
        elif isinstance(d, Tag):
            # 僅取純文字剛好等於 label 的節點，避免抓到長句子中的片段
            if d.get_text("", strip=True) == label:
                anchor = d
                break
    if not anchor:
        return None

    # 2) 從 anchor 往後找「第一個冒號」（允許全/半形），吃掉它之後開始收集值
    it = anchor.next_elements
    saw_colon = False
    parts: list[str] = []

    def cut_at_stop(s: str) -> tuple[str, bool]:
        """把 s 砍到第一個停止字元，回傳 (有效內容, 是否遇到停止)"""
        s = s.strip()
        if not s:
            return "", False
        for idx, ch in enumerate(s):
            if ch in _STOP_CHARS:
                return s[:idx], True
        return s, False

    # 3) 可能存在這種情況：label 的**下一個**文字節點一開始就是「：」或含「：...」
    for node in it:
        if isinstance(node, NavigableString):
            s = str(node)
            if not saw_colon:
                pos = s.find("：")
                if pos < 0:
                    pos = s.find(":")  # 也容許半形冒號
                if pos < 0:
                    # 還沒看到冒號，略過
                    continue
                # 吃掉冒號，冒號後可能當場就有值
                s = s[pos + 1:]
                saw_colon = True

            # 已看到冒號，開始收集值，直到遇到停止字元
            chunk, stop = cut_at_stop(s)
            if chunk:
                parts.append(chunk)
            if stop:
                break

        elif isinstance(node, Tag):
            # Tag 內的文字會在其 NavigableString 子孫節點被處理；
            # 但若是 <br>，視為硬換行不收集
            if node.name == "br":
                break

        # 安全上限，避免異常長度
        if sum(len(p) for p in parts) > maxlen * 2:
            break

    val = " ".join(p for p in parts if p).strip()
    return val[:maxlen] if val else None

def _collect_value_after_marker(el: Tag, marker_text: str, maxlen: int = 120) -> Optional[str]:
    """
    從含有 marker_text（如「學名：」「英語：」）的段落 el 內，
    沿 DOM next_elements 連續蒐集文字，直到遇到 _STOP_CHARS。
    """
    needle = None
    for d in el.descendants:
        if isinstance(d, NavigableString):
            s = str(d)
            if marker_text in s:
                needle = d
                break
    if not needle:
        return None

    parts: list[str] = []
    tail = str(needle).split(marker_text, 1)[1]

    def consume(s: str) -> tuple[str, bool]:
        s = s.strip()
        if not s:
            return "", False
        indices = [i for i, ch in enumerate(s) if ch in _STOP_CHARS]
        if indices:
            i = min(indices)
            return s[:i], True
        return s, False

    if tail:
        chunk, stop = consume(tail)
        if chunk:
            parts.append(chunk)
        if stop:
            out = " ".join(p for p in parts if p).strip()
            return out[:maxlen] if out else None

    for node in needle.next_elements:
        if isinstance(node, NavigableString):
            chunk, stop = consume(str(node))
            if chunk:
                parts.append(chunk)
            if stop:
                break
        elif isinstance(node, Tag):
            classes = " ".join(node.get("class", [])).lower()
            if "reference" in classes or "mw-editsection" in classes:
                continue
        if sum(len(p) for p in parts) > maxlen * 1.5:
            break

    out = " ".join(p for p in parts if p).strip(" ")
    return out[:maxlen] if out else None


# 標籤核心集合（語言 + 常見別名類標籤）
_LABEL_CORE_RE = r"(英語|英文|English|日語|日文|Japanese|韓語|韓文|法語|法文|德語|德文|西班牙語|西文|西語|俄語|俄文|義大利語|義文|意大利語|意文|葡萄牙語|葡文|拉丁語|拉丁文|越南語|越文|泰語|馬來語|馬來文|印尼語|印尼文|粵語|廣東話|閩南語|臺語|台語|閩南話|客語|學名|藝名|本名|原名|舊稱|又名|別名|別稱|外文)"

# 偵測「標籤：」之後接分隔符（代表缺值）
_MISSING_AFTER_LABEL = re.compile(_LABEL_CORE_RE + r"：(?=\s*[，、；)）])")

def fill_labels_if_missing(el: Tag, txt: str) -> str:
    """
    若偵測到「<標籤>：」後緊跟分隔符（值缺失），先用「標籤分節點」法補，
    若失敗再用「整段 marker」法補。
    """
    def _repl(m: re.Match) -> str:
        label = m.group(1)
        # 先用原方法找
        val = _collect_value_after_label(el, label)
        if not val:
            # 同一 text node 的標籤：冒號情況
            val = _collect_value_after_marker(el, f"{label}：")
        return f"{label}：{val}" if val else m.group(0)

    return _MISSING_AFTER_LABEL.sub(_repl, txt)

def ensure_labels_after_marker(text: str, *, title: str, session: requests.Session) -> str:
    """
    全文層級最終兜底：對仍「<語言標籤>：<缺值>」者，嘗試用 langlinks 取對應語言標題補上。
    僅對 LABEL_LANG_MAP 有對應碼的標籤生效；其他（如 藝名/本名）不在此處補。
    """
    def _repl(m: re.Match) -> str:
        label = m.group(1)
        code = LABEL_LANG_MAP.get(label)
        if not code:
            return m.group(0)
        name = _fetch_langlink_title(title, session, code)
        return f"{label}：{name}" if name else m.group(0)

    return _MISSING_AFTER_LABEL.sub(_repl, text)


def extract_specific_caption_from_tmulti(img, description_text, tmulti_container):
    """
    從 tmulti 容器的總體描述中提取特定圖片的說明
    """
    try:
        # 檢查苗栗市的格式："上：...中：...下：..."
        if "上：" in description_text and "中：" in description_text and "下：" in description_text:
            # 解析上中下格式
            sections = {}
            
            # 提取上部分
            if "上：" in description_text:
                start = description_text.find("上：") + 2
                end = description_text.find("中：")
                if end != -1:
                    sections["上"] = description_text[start:end].strip()
            
            # 提取中部分
            if "中：" in description_text:
                start = description_text.find("中：") + 2
                end = description_text.find("下：")
                if end != -1:
                    sections["中"] = description_text[start:end].strip()
            
            # 提取下部分
            if "下：" in description_text:
                start = description_text.find("下：") + 2
                sections["下"] = description_text[start:].strip()
            
            # 根據圖片位置返回對應說明
            img_position = get_image_position_in_tmulti(img, tmulti_container)
            print(f"苗栗市圖片位置: {img_position}")
            
            if img_position == 0 and "上" in sections:
                return sections["上"]
            elif img_position in [1, 2] and "中" in sections:
                # 中間部分可能有多個項目，分割處理
                middle_text = sections["中"]
                middle_items = [item.strip() for item in middle_text.split("、")]
                if len(middle_items) > 1:
                    if img_position == 1:
                        return middle_items[0]  # 苗栗巨蛋
                    elif img_position == 2:
                        return middle_items[1]  # 苗栗市天后宮
                return middle_text
            elif img_position is not None and img_position >= 3 and "下" in sections:
                # 下面部分可能有多個項目
                bottom_text = sections["下"]
                bottom_items = [item.strip() for item in bottom_text.split("、")]
                bottom_position = img_position - 3
                if bottom_position < len(bottom_items):
                    return bottom_items[bottom_position]
                return bottom_text
        
        # 檢查高雄市的格式："由左至右、從上至下：..."
        elif "由左至右" in description_text and "：" in description_text:
            # 解析 "由左至右、從上至下：高雄市區夜景、鳳山縣舊城鳳儀門、玉山主峰、衛武營國家藝術文化中心..."
            parts = description_text.split("：", 1)
            if len(parts) == 2:
                items = [item.strip() for item in parts[1].split("、")]
                
                # 嘗試確定圖片在容器中的位置
                img_position = get_image_position_in_tmulti(img, tmulti_container)
                if img_position is not None and 0 <= img_position < len(items):
                    return items[img_position]
        
        # 檢查是否包含特定關鍵詞，如衛武營
        img_src = str(img.get("src", "")).lower()
        if "wei-wu-ying" in img_src and "衛武營" in description_text:
            return "衛武營國家藝術文化中心"
        elif "kaohsiung_skyline" in img_src and ("夜景" in description_text or "市區" in description_text):
            return "高雄市區夜景"
        elif "鳳山" in description_text and ("鳳儀門" in description_text or "舊城" in description_text):
            return "鳳山縣舊城鳳儀門"
        elif "玉山" in description_text and "主峰" in description_text:
            return "玉山主峰"
        elif "龍虎塔" in description_text:
            return "龍虎塔"
        elif "光之穹頂" in description_text:
            return "光之穹頂"
        elif "體育場" in description_text or "stadium" in img_src.lower():
            return "國家體育場"
        
        # 苗栗市特定關鍵詞匹配
        elif "miaoli_station" in img_src or "tra_miaoli" in img_src:
            return "臺鐵苗栗車站"
        elif "miaoli_arena" in img_src:
            return "苗栗巨蛋"
        elif "tianhou" in img_src or "天后宮" in description_text:
            return "苗栗市天后宮"
        elif "wenchang" in img_src or "文昌祠" in description_text:
            return "苗栗文昌祠"
        elif "miaoli" in img_src and ("街景" in description_text or "panoramio" in img_src):
            return "苗栗街景"
        
    except Exception as e:
        print(f"解析 tmulti caption 時出錯: {e}")
    
    return ""

def get_image_position_in_tmulti(img, tmulti_container):
    """
    獲取圖片在 tmulti 容器中的位置（從0開始）
    """
    try:
        # 找到所有的 tsingle 元素
        tsingle_elements = tmulti_container.find_all("div", class_="tsingle")
        
        # 找到包含該圖片的 tsingle
        img_tsingle = img.find_parent("div", class_="tsingle")
        if img_tsingle and img_tsingle in tsingle_elements:
            return tsingle_elements.index(img_tsingle)
    except Exception:
        pass
    
    return None

def download_image(url: str, output_dir: Path, session: requests.Session) -> Optional[str]:
    """
    下載圖片並返回本地文件名
    """
    try:
        # 生成唯一的文件名
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        
        # 從URL獲取文件擴展名
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        if path.endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg')):
            ext = path.split('.')[-1]
        else:
            ext = 'jpg'  # 默認為jpg
        
        filename = f"{url_hash}.{ext}"
        filepath = output_dir / filename
        
        # 檢查文件是否已存在
        if filepath.exists():
            return filename
        
        # 下載圖片
        response = session.get(url, timeout=30)
        response.raise_for_status()
        
        # 確保目錄存在
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 保存文件
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        print(f"✅ 下載圖片: {filename}")
        return filename
        
    except Exception as e:
        print(f"❌ 圖片下載失敗 {url}: {e}")
        return None


def extract_images_from_infoboxes(soup: BeautifulSoup, title: str, source_url: str, 
                                 images_dir: Path, session: requests.Session) -> Tuple[list[str], list[dict]]:
    """
    從資訊框（infobox）中提取主要圖片資訊，下載圖片並返回圖片資訊
    返回: (文本中的圖片描述列表, JSONL格式的圖片資訊列表)
    """
    image_info_text = []
    image_info_json = []
    
    # 查找資訊框中的主要圖片（通常是人物照片等）
    infoboxes = soup.select("table.infobox")
    
    for infobox in infoboxes:
        # 查找資訊框中的圖片區域 - 擴展支援更多類別
        image_selectors = [
            "td.infobox-image",        # 標準人物照片
            ".infobox-image",          # 一般 infobox 圖片
            ".ib-settlement-cols-cell", # 城市/地區的圖片（如市徽）
            "td.maptable",             # 地圖表格
            ".infobox-full-data",      # 完整資料區域
            ".tmulti",                 # 多圖片容器（如城市景觀照片）
            ".thumb",                  # 縮略圖容器
            ".tsingle"                 # 單圖片容器
        ]
        
        image_cells = []
        for selector in image_selectors:
            image_cells.extend(infobox.select(selector))
        
        for cell in image_cells:
            # 查找圖片元素
            img = cell.find("img")
            if not img:
                continue
                
            # 獲取圖片URL
            src = img.get("src") or img.get("data-src")
            if not src:
                continue
            
            # 轉換為字符串並處理URL
            src = str(src)
            if src.startswith("//"):
                src = "https:" + src
            elif src.startswith("/"):
                src = "https://zh.wikipedia.org" + src
            
            # 過濾掉小圖示、編輯圖標、政黨標誌、SVG 圖片和受限制的地圖圖片
            filter_keywords = [
                "edit", "icon", "20px", "commons/thumb/8/8a/ooj",
                "emblem_of_the_kuomintang",  # 國民黨圖標
                "independent_candidate_icon", # 無黨籍圖標 
                "disambig_gray",             # 消歧義圖標
                "information_icon4",         # 資訊圖標
                "40px-", "60px-",            # 小尺寸圖片
                "chinese_characters",        # 中文字體圖片
                "characters",                # 字體圖片
                "phonetic",                  # 音標圖片
                "template",                  # 模板圖片
                "maps.wikimedia.org",        # Wikimedia 地圖瓦片 (受限制)
                "osm-intl",                  # OpenStreetMap 地圖瓦片
                "maplink",                   # 地圖連結圖片
                "mapframe"                   # 地圖框架圖片
            ]
            
            # 過濾掉 SVG 圖片
            if src.lower().endswith('.svg') or '.svg/' in src.lower():
                continue
            
            if any(keyword in src.lower() for keyword in filter_keywords):
                continue
            
            # 過濾掉圖片尺寸太小的（寬度或高度小於80px）
            try:
                width = img.get("width")
                height = img.get("height")
                if width and str(width).replace("px", "").isdigit() and int(str(width).replace("px", "")) < 80:
                    continue
                if height and str(height).replace("px", "").isdigit() and int(str(height).replace("px", "")) < 80:
                    continue
                    
                # 特別針對文字圖片的過濾：如果寬度大於高度的2倍（可能是文字圖片）
                if width and height:
                    try:
                        w = int(str(width).replace("px", ""))
                        h = int(str(height).replace("px", ""))
                        if w > 0 and h > 0:
                            ratio = w / h
                            # 如果寬高比大於3:1，很可能是文字圖片
                            if ratio > 3 and w < 300:  # 寬度小於300px 且寬高比大於3:1
                                print(f"⏭ 跳過文字圖片: {src} (尺寸: {w}x{h}, 比例: {ratio:.2f})")
                                continue
                    except (ValueError, TypeError):
                        pass
            except (ValueError, AttributeError):
                pass  # 如果無法解析尺寸，繼續處理
                
            # 嘗試獲取原始大小的圖片URL
            original_src = src
            if "/thumb/" in src and "px-" in src:
                # 從縮略圖URL推導出原始圖片URL
                # 例如: //upload.wikimedia.org/wikipedia/commons/thumb/5/57/20230630_Yeh_Shu-hua.jpg/250px-20230630_Yeh_Shu-hua.jpg
                # 變成: //upload.wikimedia.org/wikipedia/commons/5/57/20230630_Yeh_Shu-hua.jpg
                parts = src.split("/thumb/")
                if len(parts) == 2:
                    # 第二部分應該是 "5/57/20230630_Yeh_Shu-hua.jpg/250px-20230630_Yeh_Shu-hua.jpg"
                    # 我們需要去掉最後的尺寸部分，只保留 "5/57/20230630_Yeh_Shu-hua.jpg"
                    thumb_part = parts[1]
                    # 找到最後一個 "/" 並去掉後面的尺寸版本
                    last_slash = thumb_part.rfind("/")
                    if last_slash != -1:
                        file_path = thumb_part[:last_slash]  # "5/57/20230630_Yeh_Shu-hua.jpg"
                        original_src = parts[0] + "/" + file_path
            
            # 獲取圖片描述
            alt_text = img.get("alt") or "" if hasattr(img, 'get') else ""
            title_text = img.get("title") or "" if hasattr(img, 'get') else ""
            
            # 轉換為字符串
            alt_text = str(alt_text) if alt_text else ""
            title_text = str(title_text) if title_text else ""
            
            # 查找圖片說明文字
            caption = ""
            
            # 檢查是否在 .tmulti 容器中
            tmulti_container = img.find_parent("div", class_="tmulti")
            if tmulti_container:
                # 先查找該圖片對應的 .thumbcaption
                tsingle = img.find_parent("div", class_="tsingle")
                if tsingle:
                    thumbcaption = tsingle.find("div", class_="thumbcaption")
                    if thumbcaption:
                        caption = thumbcaption.get_text(strip=True)
                
                # 如果沒找到，或者只是通用文字，查找整個 tmulti 容器的總體說明
                if not caption or caption == "圖片":
                    # 查找 tmulti 容器底部的說明文字
                    # 方法1: 查找 thumbinner 後面的 div
                    thumbinner = tmulti_container.find("div", class_="thumbinner")
                    if thumbinner:
                        # 查找同級的說明元素
                        for sibling in thumbinner.find_next_siblings():
                            if hasattr(sibling, 'get_text'):
                                sibling_text = sibling.get_text(strip=True)
                                if sibling_text:
                                    print(f"找到 tmulti sibling 文字: {sibling_text}")
                                    caption = extract_specific_caption_from_tmulti(img, sibling_text, tmulti_container)
                                    if caption:
                                        print(f"提取到的 caption: {caption}")
                                        break
                    
                    # 方法2: 如果還沒找到，在整個 tmulti 容器中查找所有文字內容
                    if not caption or caption == "圖片":
                        tmulti_text = tmulti_container.get_text(strip=True)
                        print(f"tmulti 完整文字: {tmulti_text[:200]}...")
                        if tmulti_text:
                            caption = extract_specific_caption_from_tmulti(img, tmulti_text, tmulti_container)
                            if caption:
                                print(f"從完整文字提取到的 caption: {caption}")
                                    
                    # 方法3: 在同一個 infobox-full-data 中查找說明
                    if not caption or caption == "圖片":
                        infobox_data = tmulti_container.find_parent("td", class_="infobox-full-data")
                        if infobox_data:
                            # 查找說明文字（通常在圖片容器後）
                            for child in infobox_data.children:
                                if hasattr(child, 'get_text') and child != tmulti_container:
                                    child_text = child.get_text(strip=True)
                                    if child_text and len(child_text) > 10:  # 過濾短文字
                                        print(f"找到 infobox-data 文字: {child_text}")
                                        caption = extract_specific_caption_from_tmulti(img, child_text, tmulti_container)
                                        if caption:
                                            print(f"從 infobox-data 提取到的 caption: {caption}")
                                            break
            
            # 如果不在 tmulti 容器中，或者沒找到 caption，使用原有邏輯
            if not caption:
                # 在整個 infobox 中查找與此圖片相關的說明文字
                infobox = cell.find_parent("table", class_="infobox") if cell else None
                if infobox:
                    # 查找 infobox-caption 元素
                    caption_elements = infobox.find_all("div", class_="infobox-caption")
                    for cap_elem in caption_elements:
                        # 檢查說明文字是否與此圖片在同一個單元格或相近位置
                        caption_text = cap_elem.get_text(strip=True)
                        if caption_text:
                            caption = caption_text
                            break
                
                # 如果沒找到，嘗試從父級元素獲取
                if not caption:
                    parent = img.find_parent()
                    if parent and hasattr(parent, 'name') and parent.name in ["td", "th"]:
                        cell_text = parent.get_text(strip=True)
                        # 移除圖片的alt文字來獲取說明
                        if alt_text and alt_text in cell_text:
                            caption = cell_text.replace(alt_text, "").strip()
                        else:
                            caption = cell_text
            
            # 選擇最合適的描述文字
            description = caption or title_text or alt_text or "圖片"
            
            # 清理描述文字
            description = re.sub(r"\s+", " ", description.strip())
            if len(description) > 100:
                description = description[:100] + "..."
            
            # 下載圖片 - 使用原始大小的URL
            filename = download_image(original_src, images_dir, session)
            
            if filename and description:
                # 添加到JSONL資訊
                image_info_json.append({
                    "title": title,
                    "image_url": original_src,
                    "source_url": source_url,
                    "image_filename": filename,
                    "caption": description
                })
    
    return image_info_text, image_info_json


def html_to_text(html: str, title: str = "", source_url: str = "", images_dir: Optional[Path] = None, 
                 session: Optional[requests.Session] = None, exclude_sections: list[str] | None = None) -> Tuple[str, list[dict]]:
    """
    段落化輸出（REST / Action 皆可）：
    - 噪音先移除
    - 抓取導航框中的圖片資訊
    - 逐掃 h2/h3/p/ul/ol；遇到被排除章節的 h2 後直到下一個 h2 全跳過
    - **H2** 上下各留 1 空白行；其餘行單換行
    - 若偵測到「<標籤>：」後內容缺失，從該段 DOM 裡補抓值
    - 最後呼叫 zh_tidy 做標點與空白收斂
    返回: (處理後的文本, 圖片資訊JSONL列表)
    """
    soup = BeautifulSoup(html, "html.parser")

    # 在移除導航框之前，先提取圖片資訊
    nav_images_text, nav_images_json = [], []
    if images_dir and session:
        nav_images_text, nav_images_json = extract_images_from_infoboxes(
            soup, title, source_url, images_dir, session)

    # 噪音清除
    for tag in soup.select("style, script, noscript"):
        tag.decompose()
    for sel in [
        "sup.reference", "span.mw-editsection", "table.infobox", "table.navbox",
        "div.reflist", "ol.references", "div.metadata",
        # 清除維基百科警告框和編輯提示
        "div.ambox", "div.mbox-small", "div.messagebox", "table.ambox",
        "div.hatnote", "div.dablink", "div.rellink",
        # 清除 infobox 相關
        "table.infobox", "div.infobox", ".infobox",
        # 清除模板和編輯相關  
        "div.navbox", "table.navbox",
        # 清除側邊導航框
        "table.sidebar", "div.sidebar", ".sidebar"
    ]:
        for tag in soup.select(sel):
            tag.decompose()
    
    # 特殊處理可摺疊內容 - 只移除導航用的摺疊區塊，保留內容用的摺疊區塊
    for collapsible in soup.select("div.mw-collapsible"):
        # 如果是 navbox 相關的摺疊內容，則移除
        if collapsible.find_parent("div", class_="navbox") or "navbox" in " ".join(collapsible.get("class", [])):
            collapsible.decompose()
        # 如果是在表格內的摺疊內容（通常是專輯曲目），則展開而不移除
        elif collapsible.find_parent("table"):
            # 移除摺疊的樣式，讓內容完全展開
            collapsible["class"] = [c for c in collapsible.get("class", []) if c not in ["mw-collapsed", "mw-collapsible"]]

    # 選擇正確的mw-parser-output（在mw-content-text裡面的主要內容，而不是坐標指示器裡的）
    root = soup.select_one("#mw-content-text .mw-parser-output") or soup.select_one("div.mw-parser-output") or soup.body or soup
    elements = root.find_all(["h2", "h3", "p", "ul", "ol", "dl", "table"], recursive=True)

    exclude = set(exclude_sections or [])
    lines: list[str] = []
    skipping = False

    def norm_title(s: str) -> str:
        return re.sub(r"\[.*?\]", "", s).strip()

    def squeeze(s: str) -> str:
        return re.sub(r"[ \t\u00A0]+", " ", s.strip())

    # 追蹤已處理的標題，避免重複
    processed_titles = set()
    
    def add_title_if_new(title_text, prefix=""):
        """只在標題沒有重複時才添加"""
        if not title_text:
            return False
        
        # 標準化標題用於比較（移除前綴和空白）
        normalized = title_text.replace("###", "").replace("##", "").strip()
        if normalized in processed_titles:
            return False
        
        processed_titles.add(normalized)
        if lines and lines[-1] != "":
            lines.append("")
        lines.append(f"{prefix}{title_text}")
        return True

    for el in elements:
        if el.name == "h2":
            title = norm_title(smart_text(el))
            if any(key in title for key in exclude):
                skipping = True
                continue
            skipping = False
            if add_title_if_new(title, "## "):
                lines.append("")          # H2 後空行
            continue

        if el.name == "h3":
            if skipping:
                continue
            # 跳過已被 multicol 表格處理過的 h3
            if el.find_parent("table", class_="multicol"):
                continue
            title = norm_title(smart_text(el))
            add_title_if_new(title, "### ")
            continue

        if skipping:
            continue

        if el.name == "p":
            txt = squeeze(smart_text(el))
            if txt:
                # 檢查是否是簡短的標題型段落（如「個人榮譽」）
                is_likely_title = (
                    len(txt) < 50 and 
                    not txt.endswith(('。', '.', '！', '!', '？', '?')) and
                    not any(char in txt for char in '，,、；;:：')
                )
                
                if is_likely_title:
                    # 當作標題處理
                    lines.append(txt)
                else:
                    # 通用標籤補值（就地 DOM 掃描）
                    txt = fill_labels_if_missing(el, txt)
                    lines.append(txt)
        elif el.name in ("ul", "ol"):
            # 跳過已被表格處理過的 ul/ol 
            if el.find_parent("table"):
                continue
            items = []
            for li in el.find_all("li", recursive=False):
                li_txt = squeeze(smart_text(li))
                if li_txt:
                    items.append(f"• {li_txt}")
            if items:
                lines.extend(items)
        elif el.name == "dl":
            # 處理定義列表 (definition list)
            # 跳過已被表格處理過的 dl
            if el.find_parent("table"):
                continue
            for child in el.find_all(["dt", "dd"], recursive=False):
                if child.name == "dt":
                    # dt 作為小標題
                    dt_txt = squeeze(smart_text(child))
                    if dt_txt:
                        lines.append(f"### {dt_txt}")
                elif child.name == "dd":
                    # 如果 dd 包含表格，跳過它（表格會單獨處理）
                    if child.find("table"):
                        continue
                    # dd 作為內容段落
                    dd_txt = squeeze(smart_text(child))
                    if dd_txt:
                        lines.append(dd_txt)
        elif el.name == "table":
            # 只處理內容表格，跳過 infobox、navbox、ambox 等
            if el.find_parent("table") is not None:
                continue
            classes = " ".join(el.get("class", [])).lower()
            if any(cls in classes for cls in ["infobox", "navbox", "ambox", "mbox", "messagebox"]):
                continue
            
            # 特殊處理 multicol 表格（如音樂作品欄位）
            if "multicol" in classes:
                multicol_lines = process_multicol_table(el, squeeze)
                if multicol_lines:
                    lines.extend(multicol_lines)
            else:
                table_lines = table_to_lines(el, squeeze)
                if table_lines:
                    lines.extend(table_lines)

    text = "\n".join(lines)
    
    # 後處理：移除連續重複的標題
    text = remove_duplicate_headings(text)
    
    # 後處理：分離可能連在一起的標題
    text = separate_concatenated_titles(text)
    
    # 清除存檔備份文字
    text = remove_archive_links(text)
    
    text = zh_tidy(text)
    return text, nav_images_json


def remove_duplicate_headings(text: str) -> str:
    """
    移除連續重複的標題，例如：
    ### 吉他
    ### 吉他
    ### 吉他
    -> 只保留一個 ### 吉他
    """
    lines = text.split('\n')
    result_lines = []
    last_heading = None
    
    for line in lines:
        stripped = line.strip()
        
        # 檢查是否為標題行（## 或 ### 開頭）
        if stripped.startswith('###') or stripped.startswith('##'):
            # 標準化標題進行比較（移除前綴和多餘空白）
            normalized = stripped.replace('###', '').replace('##', '').strip()
            
            # 如果與上一個標題不同，或者不是標題，則添加
            if normalized != last_heading:
                result_lines.append(line)
                last_heading = normalized
            # 如果是重複標題，跳過
        else:
            # 非標題行，直接添加，並重置標題追蹤
            result_lines.append(line)
            if stripped:  # 非空行才重置
                last_heading = None
    
    return '\n'.join(result_lines)


def separate_concatenated_titles(text: str) -> str:
    """
    分離可能被連在一起的標題，例如「影音作品其他音樂錄影帶」-> 「影音作品\n其他音樂錄影帶」
    """
    # 常見的需要分離的標題模式
    title_pairs = [
        ("影音作品", "其他音樂錄影帶"),
        ("個人生活", "感情狀況"),
        ("演藝經歷", "音樂作品"),
        ("作品列表", "音樂作品"),
        ("獲獎記錄", "個人榮譽"),
    ]
    
    for title1, title2 in title_pairs:
        # 查找連在一起的標題
        combined = title1 + title2
        if combined in text:
            # 替換為分行的版本
            text = text.replace(combined, f"{title1}\n\n{title2}")
    
    return text


def remove_archive_links(text: str) -> str:
    """
    移除維基百科的存檔備份連結文字
    例如：（頁面存檔備份，存於網際網路檔案館）
    """
    # 移除存檔備份文字的多種變體
    patterns = [
        r"（頁面存檔備份，存於網際網路檔案館）",
        r"\(頁面存檔備份，存於網際網路檔案館\)",
        r"（\s*頁面存檔備份\s*，\s*存於\s*網際網路檔案館\s*）",
        r"\(\s*頁面存檔備份\s*，\s*存於\s*網際網路檔案館\s*\)",
    ]
    
    for pattern in patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)
    
    # 清理多餘的空行和空格
    text = re.sub(r"\n\s*\n\s*\n", "\n\n", text)  # 減少多餘空行
    text = re.sub(r" +", " ", text)  # 壓縮多餘空格
    
    return text


# -------------------------------
# 主流程：單篇處理
# -------------------------------
def process_one(raw: str, kind: str, out_dir: Path, session: requests.Session,
                exclude_sections: list[str], jsonl_file, source_file: str = "", force: bool = False) -> Tuple[str, bool, str]:
    title = url_to_title(raw) if kind == "url" else raw
    txt_dir = out_dir / "txt"
    images_dir = out_dir / "images"
    images_jsonl = out_dir / "images" / "images_info.jsonl"

    requested_filename = safe_filename(title)
    requested_path = txt_dir / f"{requested_filename}.txt"
    # 如果 force=True，無論文件是否存在都要處理；如果 force=False 且文件存在，則跳過
    if not force and requested_path.exists():
        return title, True, "exists"

    # 先 Action（variant=zh-tw），失敗再 REST（帶 Accept-Language: zh-tw）
    html = None
    actual_title = title
    source_url = f"https://zh.wikipedia.org/wiki/{quote(title, safe='')}"
    
    try:
        html, actual_title = fetch_html_action(title, session)
    except Exception:
        html, actual_title = fetch_html_rest(title, session)

    # 檢查HTML層面的重定向（特別是REST API返回的重定向頁面）
    html_redirect_target = detect_redirect_from_html(html)
    if html_redirect_target and html_redirect_target != title:
        print(f"🔄 檢測到HTML重定向：{title} -> {html_redirect_target}")
        source_url = f"https://zh.wikipedia.org/wiki/{quote(html_redirect_target, safe='')}"
        
        try:
            html, actual_title = fetch_html_action(html_redirect_target, session)
        except Exception:
            try:
                html, actual_title = fetch_html_rest(html_redirect_target, session)
            except Exception as e:
                raise RuntimeError(f"重定向目標頁面抓取失敗: {html_redirect_target}, 錯誤: {e}")

    text, image_info = html_to_text(html, title=actual_title, source_url=source_url, 
                                   images_dir=images_dir, session=session, 
                                   exclude_sections=exclude_sections)

    redirect_target = detect_redirect(text)
    if redirect_target and redirect_target != title:
        print(f"🔄 檢測到重定向：{title} -> {redirect_target}")
        source_url = f"https://zh.wikipedia.org/wiki/{quote(redirect_target, safe='')}"
        
        try:
            html, actual_title = fetch_html_action(redirect_target, session)
        except Exception:
            try:
                html, actual_title = fetch_html_rest(redirect_target, session)
            except Exception as e:
                raise RuntimeError(f"重定向目標頁面抓取失敗: {redirect_target}, 錯誤: {e}")

        text, image_info = html_to_text(html, title=actual_title, source_url=source_url,
                                       images_dir=images_dir, session=session,
                                       exclude_sections=exclude_sections)

    # 全文兜底：若仍有「<語言標籤>：<缺值>」，用 langlinks 補
    text = ensure_labels_after_marker(text, title=actual_title, session=session)

    # 檢查文本長度，如果為 0 則拋出錯誤
    if len(text.strip()) == 0:
        raise RuntimeError(f"抓取的文本長度為 0：{actual_title}")

    final_filename = safe_filename(actual_title)
    out_txt = txt_dir / f"{final_filename}.txt"
    status = "redirect_ok" if redirect_target else "ok"

    # 如果 force=True，無論文件是否存在都要覆蓋；如果 force=False 且文件存在，則跳過
    if not force and out_txt.exists():
        if redirect_target:
            return actual_title, True, "redirect_exists"
        return actual_title, True, "exists"

    out_txt.parent.mkdir(parents=True, exist_ok=True)
    out_txt.write_text(text, encoding="utf-8")

    # 保存圖片資訊到JSONL文件
    if image_info:
        images_jsonl.parent.mkdir(parents=True, exist_ok=True)
        with images_jsonl.open("a", encoding="utf-8") as img_file:
            for img_data in image_info:
                img_file.write(json.dumps(img_data, ensure_ascii=False) + "\n")

    rec = {
        "title": actual_title,
        "original_query": title,
        "source_url": f"https://zh.wikipedia.org/wiki/{quote(actual_title, safe='')}",
        "variant": "zh-tw",
        "source_title_lang": "zh-tw",
        "text_length": len(text),
        "text": text,
        "out_file": str(Path("txt") / f"{final_filename}.txt"),
        "source_file": source_file,  # 新增來源檔案資訊
    }
    if redirect_target:
        rec["redirected_from"] = title
        rec["redirected_to"] = actual_title
    
    # 添加圖片資訊到記錄中
    if image_info:
        rec["images_count"] = len(image_info)
        rec["images"] = [{"filename": img["image_filename"], "caption": img["caption"]} 
                        for img in image_info]

    if jsonl_file is not None:
        jsonl_file.write(json.dumps(rec, ensure_ascii=False) + "\n")
        jsonl_file.flush()

    return actual_title, True, status


# -------------------------------
# CLI 入口
# -------------------------------
def main():
    ap = argparse.ArgumentParser(description="批次抓取中文維基（臺灣正體 zh-TW）")
    ap.add_argument("--targets", required=True, help="目標清單路徑：.txt 或 .jsonl 檔案，或包含多個 .txt/.jsonl 檔案的資料夾路徑")
    ap.add_argument("--out-dir", default="out", help="輸出資料夾")
    ap.add_argument("--sleep", type=float, default=0.5, help="每篇之間的延遲秒數（禮貌抓取）")
    ap.add_argument("--ua", default=DEFAULT_UA, help="自訂 User-Agent（請填可聯絡資訊）")
    ap.add_argument("--force", action="store_true", help="強制重新下載已存在的檔案")
    ap.add_argument(
        "--exclude-sections",
        # zh 變體會把「相关条目/扩展阅读」自動轉為「相關條目/擴展閱讀」
        default="引用資料,參考書目,相關學術研究書目,參考,參考來源,參考資料,外部連結,相關條目,擴展閱讀,延伸閱讀,參見,參考文獻,腳註,註釋,註解,注解,備註,關聯項目,資料來源,注釋,註腳,注腳,關連項目,備注,備註",
        help="要排除的章節標題（以逗號分隔）",
    )
    args = ap.parse_args()

    targets_path = Path(args.targets)
    if not targets_path.exists():
        print(f"❌ 目標路徑不存在: {targets_path}")
        return
        
    out_dir = Path(args.out_dir)
    exclude_sections = [s.strip() for s in args.exclude_sections.split(",") if s.strip()]

    S = requests.Session()
    S.headers.update({
        "User-Agent": args.ua,
        # 讓 REST/Parsoid 依變體輸出臺灣正體；與 Action API 的 variant 相輔相成
        "Accept-Language": "zh-tw",
    })

    ok, skip, fail = 0, 0, 0
    current_source_file = ""
    failures_log = out_dir / "_failures.jsonl"
    txt_dir = out_dir / "txt"
    json_dir = out_dir / "jsonl"
    images_dir = out_dir / "images"
    all_data_jsonl = json_dir / "all_data.jsonl"
    images_jsonl = images_dir / "images_info.jsonl"

    out_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)
    
    # 先計算總數量，以便顯示進度
    all_targets = list(iter_targets(targets_path))
    total_count = len(all_targets)
    processed_count = 0
    
    print(f"🚀 開始處理 {total_count} 個目標")
    
    import io
    for raw, kind, source_file in all_targets:
        processed_count += 1
        
        # 顯示當前處理的檔案（如果變更）
        if source_file != current_source_file:
            current_source_file = source_file
            print(f"\n📂 當前處理檔案: {Path(source_file).name}")
        
        try:
            # 使用 StringIO 暫存 jsonl 記錄
            rec_io = io.StringIO()
            title, success, msg = process_one(raw, kind, out_dir, S, exclude_sections, rec_io, source_file, args.force)
            
            # 解析新產生的記錄
            rec_io.seek(0)
            new_rec = None
            for line in rec_io:
                if line.strip():
                    new_rec = json.loads(line)
                    break
            
            # 只有成功處理且有新記錄時才寫入 jsonl
            if new_rec and success and msg not in ["exists", "redirect_exists"]:
                if args.force:
                    # force 模式：讀取現有 jsonl，移除同 title 的舊記錄，再寫回全部
                    all_jsonl = []
                    if all_data_jsonl.exists():
                        with all_data_jsonl.open("r", encoding="utf-8") as f:
                            for line in f:
                                if line.strip():
                                    try:
                                        obj = json.loads(line)
                                        if obj.get("title") != title:
                                            all_jsonl.append(obj)
                                    except Exception:
                                        pass
                    all_jsonl.append(new_rec)
                    with all_data_jsonl.open("w", encoding="utf-8") as f:
                        for obj in all_jsonl:
                            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
                else:
                    # 非 force 模式：直接追加
                    with all_data_jsonl.open("a", encoding="utf-8") as f:
                        f.write(json.dumps(new_rec, ensure_ascii=False) + "\n")
            
            if msg == "exists":
                skip += 1
                print(f"⏭️  [{processed_count}/{total_count}] 略過（已存在）：{title}")
            elif msg == "redirect_exists":
                skip += 1
                print(f"⏭️  [{processed_count}/{total_count}] 略過（重定向目標已存在）：{title}")
            elif msg == "redirect_ok":
                ok += 1
                print(f"✅ [{processed_count}/{total_count}] 完成（重定向）：{raw} -> {title}")
            else:
                ok += 1
                print(f"✅ [{processed_count}/{total_count}] 完成：{title}")
        except Exception as e:
            fail += 1
            print(f"❌  [{processed_count}/{total_count}] 失敗：{raw}  -> {e}")
            failures_log.parent.mkdir(parents=True, exist_ok=True)
            with failures_log.open("a", encoding="utf-8") as f:
                f.write(json.dumps({"raw": raw, "kind": kind, "source_file": source_file, "error": str(e)}, ensure_ascii=False) + "\n")
        time.sleep(args.sleep)

    print("\n=== 統計 ===")
    print(f"成功：{ok}  已存在：{skip}  失敗：{fail}")
    print(f"文字輸出：{txt_dir.resolve()}")
    print(f"JSONL 輸出：{all_data_jsonl.resolve()}")
    if images_jsonl.exists():
        print(f"圖片資訊：{images_jsonl.resolve()}")
        print(f"圖片下載：{images_dir.resolve()}")


if __name__ == "__main__":
    main()

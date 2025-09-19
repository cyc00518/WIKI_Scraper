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
def iter_targets(path: Path) -> Iterable[Tuple[str, str]]:
    """
    讀取 .txt 或 .jsonl 目標清單。
    產出 (raw, kind)：kind = "url" 或 "title"
    """
    if path.suffix.lower() == ".jsonl":
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                obj = json.loads(line)
                if "url" in obj and obj["url"]:
                    yield obj["url"], "url"
                elif "title" in obj and obj["title"]:
                    yield obj["title"], "title"
    else:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                if s.startswith("http://") or s.startswith("https://"):
                    yield s, "url"
                else:
                    yield s, "title"


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
def fetch_html_rest(title: str, session: requests.Session, timeout=30) -> str:
    url = REST_HTML.format(title=quote(title, safe=""))
    r = http_get_with_backoff(session, url, timeout=timeout)
    return r.text

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


def fetch_html_action(title: str, session: requests.Session, timeout=30) -> Tuple[str, str]:
    """
    抓取頁面內容，同時返回實際的標題（處理重定向）
    返回: (html_content, actual_title)
    """
    params = {
        "action": "parse",
        "page": title,
        "prop": "text",
        "format": "json",
        "variant": "zh-tw",  # 指定臺灣正體變體
        "maxlag": 5,
    }
    r = http_get_with_backoff(session, API, params=params, timeout=timeout)
    data = r.json()
    if "parse" not in data or "text" not in data["parse"]:
        raise RuntimeError(f"Action parse missing content for: {title}")
    
    # 獲取實際的頁面標題（處理重定向）
    actual_title = data["parse"].get("title", title)
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
    """
    parts = []
    last_ascii = False
    for d in node.descendants:
        if isinstance(d, NavigableString):
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
            parts.append("\n")
            last_ascii = False
    return "".join(parts)


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


def html_to_text(html: str, exclude_sections: list[str] | None = None) -> str:
    """
    段落化輸出（REST / Action 皆可）：
    - 噪音先移除
    - 逐掃 h2/h3/p/ul/ol；遇到被排除章節的 h2 後直到下一個 h2 全跳過
    - **H2** 上下各留 1 空白行；其餘行單換行
    - 若偵測到「<標籤>：」後內容缺失，從該段 DOM 裡補抓值
    - 最後呼叫 zh_tidy 做標點與空白收斂
    """
    soup = BeautifulSoup(html, "html.parser")

    # 噪音清除
    for sel in [
        "sup.reference", "span.mw-editsection", "table.infobox", "table.navbox",
        "div.reflist", "ol.references", "div.metadata",
        # 清除維基百科警告框和編輯提示
        "div.ambox", "div.mbox-small", "div.messagebox", "table.ambox",
        "div.hatnote", "div.dablink", "div.rellink",
        # 清除 infobox 相關
        "table.infobox", "div.infobox", ".infobox",
        # 清除模板和編輯相關
        "div.navbox", "table.navbox", "div.mw-collapsible"
    ]:
        for tag in soup.select(sel):
            tag.decompose()

    root = soup.select_one("div.mw-parser-output") or soup.body or soup
    elements = root.find_all(["h2", "h3", "p", "ul", "ol", "table"], recursive=True)

    exclude = set(exclude_sections or [])
    lines: list[str] = []
    skipping = False

    def norm_title(s: str) -> str:
        return re.sub(r"\[.*?\]", "", s).strip()

    def squeeze(s: str) -> str:
        return re.sub(r"[ \t\u00A0]+", " ", s.strip())

    for el in elements:
        if el.name == "h2":
            title = norm_title(smart_text(el))
            if any(key in title for key in exclude):
                skipping = True
                continue
            skipping = False
            if lines and lines[-1] != "":
                lines.append("")          # H2 前空行
            if title:
                lines.append(squeeze(title))
                lines.append("")          # H2 後空行
            continue

        if el.name == "h3":
            if skipping:
                continue
            title = norm_title(smart_text(el))
            if title:
                # 確保H3前有適當的分隔（如果前面不是空行的話）
                if lines and lines[-1] != "":
                    lines.append("")
                lines.append(squeeze(title))
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
            items = []
            for li in el.find_all("li", recursive=False):
                li_txt = squeeze(smart_text(li))
                if li_txt:
                    items.append(f"• {li_txt}")
            if items:
                lines.extend(items)
        elif el.name == "table":
            # 只處理內容表格，跳過 infobox、navbox、ambox 等
            classes = " ".join(el.get("class", [])).lower()
            if any(cls in classes for cls in ["infobox", "navbox", "ambox", "mbox", "messagebox"]):
                continue
            
            # 處理表格：將每行轉換為「• 列1 | 列2 | 列3」格式
            table_lines = []
            for row in el.find_all("tr"):
                cells = []
                for cell in row.find_all(["th", "td"]):
                    cell_txt = squeeze(smart_text(cell))
                    if cell_txt:
                        cells.append(cell_txt)
                if cells:
                    table_lines.append("• " + " | ".join(cells))
            if table_lines:
                lines.extend(table_lines)

    text = "\n".join(lines)
    
    # 後處理：分離可能連在一起的標題
    text = separate_concatenated_titles(text)
    text = zh_tidy(text)
    return text


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


# -------------------------------
# 主流程：單篇處理
# -------------------------------
def process_one(raw: str, kind: str, out_dir: Path, session: requests.Session,
                exclude_sections: list[str], jsonl_file) -> Tuple[str, bool, str]:
    title = url_to_title(raw) if kind == "url" else raw
    filename = safe_filename(title)
    out_txt = out_dir / f"{filename}.txt"

    if out_txt.exists():
        return title, True, "exists"

    # 先 Action（variant=zh-tw），失敗再 REST（帶 Accept-Language: zh-tw）
    html = None
    actual_title = title
    try:
        html, actual_title = fetch_html_action(title, session)
    except Exception:
        html = fetch_html_rest(title, session)

    text = html_to_text(html, exclude_sections=exclude_sections)
    
    # 檢測是否為重定向頁面
    redirect_target = detect_redirect(text)
    if redirect_target and redirect_target != title:
        print(f"🔄 檢測到重定向：{title} -> {redirect_target}")
        # 抓取重定向目標頁面
        try:
            html, actual_title = fetch_html_action(redirect_target, session)
        except Exception:
            try:
                html = fetch_html_rest(redirect_target, session)
                actual_title = redirect_target
            except Exception as e:
                raise RuntimeError(f"重定向目標頁面抓取失敗: {redirect_target}, 錯誤: {e}")
        
        text = html_to_text(html, exclude_sections=exclude_sections)
        # 更新文件名為實際目標頁面
        filename = safe_filename(actual_title)
        out_txt = out_dir / f"{filename}.txt"
        
        # 如果目標文件已存在，跳過
        if out_txt.exists():
            return actual_title, True, "redirect_exists"

    # 全文兜底：若仍有「<語言標籤>：<缺值>」，用 langlinks 補
    text = ensure_labels_after_marker(text, title=actual_title, session=session)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_txt.write_text(text, encoding="utf-8")
    
    # 寫入全域 jsonl 文件
    rec = {
        "title": actual_title,
        "original_query": title if redirect_target else actual_title,
        "source_url": f"https://zh.wikipedia.org/wiki/{quote(actual_title, safe='')}",
        "variant": "zh-tw",
        "text_length": len(text),
        "text": text,
    }
    if redirect_target:
        rec["redirected_from"] = title
        rec["redirected_to"] = actual_title
    
    jsonl_file.write(json.dumps(rec, ensure_ascii=False) + "\n")
    jsonl_file.flush()  # 確保立即寫入
    
    status = "redirect_ok" if redirect_target else "ok"
    return actual_title, True, status


# -------------------------------
# CLI 入口
# -------------------------------
def main():
    ap = argparse.ArgumentParser(description="批次抓取中文維基（臺灣正體 zh-TW）")
    ap.add_argument("--targets", required=True, help="目標清單路徑：.txt 或 .jsonl")
    ap.add_argument("--out-dir", default="out", help="輸出資料夾")
    ap.add_argument("--sleep", type=float, default=1.0, help="每篇之間的延遲秒數（禮貌抓取）")
    ap.add_argument("--ua", default=DEFAULT_UA, help="自訂 User-Agent（請填可聯絡資訊）")
    ap.add_argument(
        "--exclude-sections",
        # zh 變體會把「相关条目/扩展阅读」自動轉為「相關條目/擴展閱讀」
        default="參考書目,相關學術研究書目,參考來源,參考資料,外部連結,相關條目,擴展閱讀,延伸閱讀,參見,參考文獻,腳註,註釋,註解,注解,備註,關聯項目,資料來源,注釋,註腳,注腳,關連項目",
        help="要排除的章節標題（以逗號分隔）",
    )
    args = ap.parse_args()

    targets_path = Path(args.targets)
    out_dir = Path(args.out_dir)
    exclude_sections = [s.strip() for s in args.exclude_sections.split(",") if s.strip()]

    S = requests.Session()
    S.headers.update({
        "User-Agent": args.ua,
        # 讓 REST/Parsoid 依變體輸出臺灣正體；與 Action API 的 variant 相輔相成
        "Accept-Language": "zh-tw",
    })

    ok, skip, fail = 0, 0, 0
    failures_log = out_dir / "_failures.jsonl"
    all_data_jsonl = out_dir / "all_data.jsonl"
    
    # 創建全域 jsonl 文件（使用 append 模式，避免每次覆寫）
    out_dir.mkdir(parents=True, exist_ok=True)
    with all_data_jsonl.open("a", encoding="utf-8") as jsonl_file:
        for raw, kind in iter_targets(targets_path):
            try:
                title, success, msg = process_one(raw, kind, out_dir, S, exclude_sections, jsonl_file)
                if msg == "exists":
                    skip += 1
                    print(f"⏭️  略過（已存在）：{title}")
                elif msg == "redirect_exists":
                    skip += 1
                    print(f"⏭️  略過（重定向目標已存在）：{title}")
                elif msg == "redirect_ok":
                    ok += 1
                    print(f"✅ 完成（重定向）：{raw} -> {title}")
                else:
                    ok += 1
                    print(f"✅ 完成：{title}")
            except Exception as e:
                fail += 1
                print(f"❌  失敗：{raw}  -> {e}")
                failures_log.parent.mkdir(parents=True, exist_ok=True)
                with failures_log.open("a", encoding="utf-8") as f:
                    f.write(json.dumps({"raw": raw, "kind": kind, "error": str(e)}, ensure_ascii=False) + "\n")
            time.sleep(args.sleep)

    print("\n=== 統計 ===")
    print(f"成功：{ok}  已存在：{skip}  失敗：{fail}")
    print(f"輸出資料夾：{out_dir.resolve()}")


if __name__ == "__main__":
    main()
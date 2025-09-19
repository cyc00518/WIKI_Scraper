# Wiki crawler 修正待辦清單

以下是根據你提出的需求拆解的待辦事項與詳細執行想法（先不做程式修改），並以 checkbox 樣式呈現；完成項目會打勾。

## 目標
修正爬蟲輸出：表格解析正確、移除殘留 CSS/HTML 類文字、使用繁體 `zh-tw` 作為檔名與 `title`，並分開儲存 `.txt` 與 `all_data.jsonl`。

## 待辦清單

- [x] 1. 建立 `todo-list.md`
   - 已完成（本檔案）。

- [x] 2. 重現與分析表格問題
   - 已對 `out/ONE PIECE.txt` 進行逐段檢視，確認表格 flatten 丟失跨欄資訊並記錄需求。
   - 由於目前環境無法連線維基，改採舊輸出與模擬 HTML 驗證行為。

- [x] 3. 清除 `.mw-parser-output` 與內嵌樣式
   - 在 `html_to_text()` 初始階段移除 `<style> / <script> / <noscript>`，輸出不再含 `.mw-parser-output …`。

- [x] 4. 修正表格解析與格式化邏輯
   - 新增 `table_to_lines()`，完整展開 `rowspan/colspan` 並輸出 `• 書名 | 發售日期 | ISBN` 等欄位。
   - 處理嵌套表格與 caption，避免重複輸出。

- [x] 5. 使用 `zh-tw` 作為檔名與 `title`
   - 透過 `displaytitle`/REST meta 抓取繁體顯示標題，`process_one()` 使用其產出檔名與 JSON `title`。

- [x] 6. 分開存放 `all_data.jsonl` 與 `.txt`
   - CLI 會建立 `out/txt` 與 `out/jsonl/all_data.jsonl`，`process_one()` 回報 `out_file` 相對路徑。

- [ ] 7. 整合測試與驗證
   - 目前環境 DNS 封鎖，`targets_test.txt` 無法完成實際抓取；已透過本地舊輸出與模擬 HTML 驗證邏輯。
   - 待能連線時重新跑 CLI，確認 `out/txt/IU_(歌手).txt` 與 JSONL 均為繁體並檢視表格。

## 風險與備註

- 若 MediaWiki API 回傳內容本身已是簡體，可能需要額外對照繁簡轉換（使用 `opencc`）——先嘗試 `uselang=zh-tw`。
- 表格欄位複雜（多重 `rowspan`/`colspan` 或不規則表格）可能需做 heuristic 手工處理。

## 下一步（提案）

- 取得 sample 原始 HTML（`ONE PIECE` 範例）與目前輸出，做差異分析（步驟 2）。
- 根據分析結果實作第 3 與第 4 項。

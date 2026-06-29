# -*- coding: utf-8 -*-
from docx import Document
from docx.shared import Pt, RGBColor, Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml.ns import qn
from docx.oxml import OxmlElement

ACCENT = RGBColor(0x1F, 0x4E, 0x79)
GREY = RGBColor(0x55, 0x55, 0x55)
LIGHT = "DCE6F1"

doc = Document()

# 預設字型（中英）
style = doc.styles['Normal']
style.font.name = 'Microsoft JhengHei'
style.font.size = Pt(10.5)
style.element.rPr.rFonts.set(qn('w:eastAsia'), 'Microsoft JhengHei')

def set_cell_bg(cell, color):
    tcPr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement('w:shd')
    shd.set(qn('w:val'), 'clear')
    shd.set(qn('w:fill'), color)
    tcPr.append(shd)

def h1(text):
    p = doc.add_paragraph()
    r = p.add_run(text)
    r.bold = True
    r.font.size = Pt(16)
    r.font.color.rgb = ACCENT
    p.space_after = Pt(6)
    # 底線
    pPr = p._p.get_or_add_pPr()
    pbdr = OxmlElement('w:pBdr')
    bottom = OxmlElement('w:bottom')
    bottom.set(qn('w:val'), 'single')
    bottom.set(qn('w:sz'), '12')
    bottom.set(qn('w:color'), '1F4E79')
    bottom.set(qn('w:space'), '4')
    pbdr.append(bottom)
    pPr.append(pbdr)
    return p

def h2(text):
    p = doc.add_paragraph()
    r = p.add_run(text)
    r.bold = True
    r.font.size = Pt(12.5)
    r.font.color.rgb = ACCENT
    return p

def para(text, bold=False, color=None, size=10.5):
    p = doc.add_paragraph()
    r = p.add_run(text)
    r.bold = bold
    r.font.size = Pt(size)
    if color:
        r.font.color.rgb = color
    return p

def bullet(text, sub=False):
    p = doc.add_paragraph(style='List Bullet' + (' 2' if sub else ''))
    r = p.add_run(text)
    r.font.size = Pt(10.5)
    return p

def kv_table(rows):
    t = doc.add_table(rows=0, cols=2)
    t.alignment = WD_TABLE_ALIGNMENT.LEFT
    t.style = 'Light Grid Accent 1'
    for k, v in rows:
        cells = t.add_row().cells
        cells[0].width = Inches(1.6)
        cells[1].width = Inches(4.8)
        rk = cells[0].paragraphs[0].add_run(k)
        rk.bold = True
        rk.font.size = Pt(10)
        cells[1].paragraphs[0].add_run(v).font.size = Pt(10)
    doc.add_paragraph()
    return t

# ===== 封面 =====
title = doc.add_paragraph()
title.alignment = WD_ALIGN_PARAGRAPH.CENTER
r = title.add_run('智能投資分析平台')
r.bold = True
r.font.size = Pt(30)
r.font.color.rgb = ACCENT

sub = doc.add_paragraph()
sub.alignment = WD_ALIGN_PARAGRAPH.CENTER
r = sub.add_run('模組功能說明與操作手冊')
r.font.size = Pt(16)
r.font.color.rgb = GREY

doc.add_paragraph()
meta = doc.add_paragraph()
meta.alignment = WD_ALIGN_PARAGRAPH.CENTER
meta.add_run('台股 / 美股雙市場  ·  AI 自動化操盤  ·  即時行情整合\n').font.size = Pt(11)
meta.add_run('版本日期：2026-06-29（含操盤強化層更新）').font.size = Pt(10)

doc.add_paragraph()
note = doc.add_paragraph()
note.alignment = WD_ALIGN_PARAGRAPH.CENTER
rn = note.add_run('本平台以免費 yfinance 行情為資料來源，結合 Claude AI 進行盤勢研判，\n涵蓋選股、預測、回測、自動化操盤與每日盤後總結的完整投資工作流。')
rn.font.size = Pt(10)
rn.font.color.rgb = GREY

doc.add_page_break()

# ===== 目錄式總覽 =====
h1('一、平台總覽')
para('本平台是一套以網頁操作的個人投資決策系統，共分為三大類、十二個功能模組。所有頁面共用上方導覽列，可隨時切換。每一檔股票（台股或美股）在各頁清單中皆可點擊，滑出「個股詳情面板」查看 K 線圖、技術指標、新聞與籌碼。')
doc.add_paragraph()

h2('模組地圖')
ov = doc.add_table(rows=1, cols=3)
ov.style = 'Light List Accent 1'
hdr = ov.rows[0].cells
for i, t in enumerate(['分類', '模組', '一句話功能']):
    set_cell_bg(hdr[i], '1F4E79')
    p = hdr[i].paragraphs[0]
    rr = p.add_run(t); rr.bold = True; rr.font.color.rgb = RGBColor(0xFF,0xFF,0xFF); rr.font.size = Pt(10.5)

ov_rows = [
    ('市場儀表', '美股儀表板', '美股總覽、指數、個股快速查詢'),
    ('市場儀表', '台股儀表板', '台股總覽、即時報價與監測清單'),
    ('市場儀表', '投資組合管理', '記錄持股、計算損益與配置'),
    ('選股工具', '策略選股', '依條件智能篩選台 / 美股'),
    ('選股工具', '績優股清單', '財報狗式多因子選出被低估績優股'),
    ('選股工具', '低位階永動機', '低接起漲選股與長期監測（春燕來了法）'),
    ('AI 操盤', 'AI 明日預測', '單檔技術 + 消息面的明日傾向分析'),
    ('AI 操盤', 'AI 自動化 Agent', '自動晨報、盤中、盤後的 AI 操盤助理'),
    ('AI 操盤', 'AI 智能投資中心', 'AI 功能入口與綜合建議'),
    ('AI 操盤', '目標導向操盤', '設定獲利目標、產生今晚操盤總結'),
    ('進階', '策略回測', '驗證選股 / 操盤策略的歷史績效'),
    ('進階', '策略工作台', '自訂與組合策略條件'),
]
for c, m, d in ov_rows:
    cells = ov.add_row().cells
    cells[0].paragraphs[0].add_run(c).font.size = Pt(9.5)
    rb = cells[1].paragraphs[0].add_run(m); rb.bold = True; rb.font.size = Pt(9.5)
    cells[2].paragraphs[0].add_run(d).font.size = Pt(9.5)
doc.add_paragraph()
doc.add_page_break()

# ===== 共通操作 =====
h1('二、共通操作（先讀這段）')
h2('個股詳情面板（全站通用）')
para('在任何清單頁，點擊某一檔股票的列，畫面右側會滑出「詳情抽屜」，這是全站最常用的功能。')
bullet('K線分頁（預設開啟）：自繪蠟燭圖，內含 MA5 / MA20 / MA60 與年線 MA240。')
bullet('時間框切換：日K·3月 / 日K·6月 / 週K 三種，點按鈕即時重畫。')
bullet('明日傾向：逐條列出「站上月線 / MACD / KD / RSI / 均線多排」，並標示年線位階（現價 vs 年線，判斷是否為低位階）。')
bullet('新聞與籌碼：台股顯示法人 / 借券；美股顯示基本面與新聞標題。')
bullet('「AI 明日預測」按鈕：點擊會帶入該檔代號跳到預測頁自動分析。')
doc.add_paragraph()

h2('輸入股票代號的方式')
bullet('美股：直接輸入代號，例如 AAPL、NVDA、TSLA。')
bullet('台股：輸入四位數字即可，例如 2330、2317，系統會自動補上市場後綴並顯示中文名稱。')
doc.add_paragraph()

h2('省 Token 的監測開關（重要）')
para('AI 分析會消耗運算額度（Token），平台提供逐檔開關，避免不必要的花費：')
bullet('監測清單的每一檔都有「啟用 / 停用」開關，停用後掃描會跳過、不呼叫 AI。')
bullet('可一鍵「全部暫停」監測。')
bullet('持倉表有「AI監測」欄，關閉後該檔在晨報 / 盤中 / 盤後都不會被 AI 重新分析。')
doc.add_page_break()

# ===== 模組逐一 =====
def module(num, name, route, what, ops, tips=None):
    h1(f'{num}、{name}')
    kv_table([('頁面路徑', route), ('用途', what)])
    h2('重點功能')
    return

h1('三、各模組功能與操作')

# 1 美股儀表板
h2('1. 美股儀表板')
kv_table([('路徑', '/  （首頁）'), ('定位', '美股市場的總覽入口')])
para('重點功能', bold=True, color=ACCENT)
bullet('顯示美股大盤指數、熱門個股與市場概況。')
bullet('輸入任一美股代號即可快速查詢報價與走勢。')
bullet('清單列可點開個股詳情面板。')
para('操作步驟', bold=True, color=ACCENT)
bullet('開啟首頁，於搜尋框輸入代號（如 NVDA）後送出。')
bullet('點擊清單中的個股 → 右側滑出詳情面板看 K 線與技術指標。')
doc.add_paragraph()

# 2 台股儀表板
h2('2. 台股儀表板')
kv_table([('路徑', '/tw'), ('定位', '台股市場總覽與監測中樞')])
para('重點功能', bold=True, color=ACCENT)
bullet('台股大盤、個股即時報價與中文名稱顯示。')
bullet('維護「監測清單」，並可逐檔開 / 關 AI 監測、全部暫停。')
bullet('整合法人、借券等籌碼資訊。')
para('操作步驟', bold=True, color=ACCENT)
bullet('輸入四位代號（如 2330）查詢台積電。')
bullet('將個股加入監測清單；用每檔的開關控制是否要 AI 持續盯盤（省 Token）。')
doc.add_paragraph()

# 3 投資組合
h2('3. 投資組合管理')
kv_table([('路徑', '/portfolio'), ('定位', '記錄你的實際持股與損益')])
para('重點功能', bold=True, color=ACCENT)
bullet('登錄買進的股票、股數與成本。')
bullet('自動以最新報價計算未實現損益、配置比例。')
bullet('持股列同樣可點開詳情面板。')
para('操作步驟', bold=True, color=ACCENT)
bullet('新增持股：輸入代號、股數、買進價。')
bullet('系統即時計算總市值與損益，定期回來檢視即可。')
doc.add_paragraph()

# 4 策略選股
h2('4. 策略選股')
kv_table([('路徑', '/screener'), ('定位', '依條件智能篩選台 / 美股')])
para('重點功能', bold=True, color=ACCENT)
bullet('設定篩選條件（技術、價量等），一鍵跑出符合的股票清單。')
bullet('台股 / 美股皆可切換。')
bullet('篩選結果會自動保存，跳頁或關分頁再回來仍在。')
para('操作步驟', bold=True, color=ACCENT)
bullet('選擇市場 → 設定條件 → 按「篩選」。')
bullet('在結果清單點任一檔開詳情面板進一步研究。')
doc.add_paragraph()

# 5 績優股清單
h2('5. 績優股清單（財報狗選股法）')
kv_table([('路徑', '/bluechip'), ('定位', '用多因子選出「被低估的績優股」')])
para('重點功能', bold=True, color=ACCENT)
bullet('沿用財報狗（Statementdog）選股邏輯：先剔除自由現金流報酬率衰退的公司。')
bullet('以三年平均自由現金流報酬率、本益比、股價淨值比、殖利率綜合排名。')
bullet('綜合分數越小代表越被低估，排在越前面。')
bullet('預設台股，可切換美股。')
para('操作步驟', bold=True, color=ACCENT)
bullet('選擇市場 → 按「執行選股」→ 等待跑分（單檔結果快取 6 小時，第二次更快）。')
bullet('查看排名表，點選有興趣的個股看詳情。')
doc.add_paragraph()

# 6 低位階永動機
h2('6. 低位階永動機（春燕來了 低位階長期永動機投資法）')
kv_table([('路徑', '/perpetual'), ('定位', '低接起漲的選股與長期監測（台股）')])
para('重點功能', bold=True, color=ACCENT)
bullet('依「春燕來了」的低位階投資法：以五年區間計算股價位階，鎖定相對歷史底部。')
bullet('判斷是否在年線（MA240）之下、週KD 是否處於低檔。')
bullet('給出進場 / 續抱 / 減碼 / 出場訊號；持倉跌破 -20% 觸發硬停損提醒。')
bullet('位階 =（現價 − 五年低點）/（五年高點 − 五年低點），數值越低越靠近底部。')
para('操作步驟', bold=True, color=ACCENT)
bullet('按「選股」找出目前低位階、有起漲跡象的標的。')
bullet('將標的加入監測，系統會持續判斷訊號。')
bullet('停損紀律：-10% 可接受、-20% 為極限。')
doc.add_paragraph()

# 7 AI 明日預測
h2('7. AI 明日預測分析師')
kv_table([('路徑', '/predict'), ('定位', '單檔的明日傾向 AI 研判')])
para('重點功能', bold=True, color=ACCENT)
bullet('結合技術面（均線 / MACD / KD / RSI）、起漲評分與年線位階。')
bullet('AI 會同時參考隔夜美股（費半 / 那指 / 標普 / 台積電 ADR）、美元台幣、個股新聞、借券 / 法人連續、月營收 YoY-MoM 與 EPS。')
bullet('區分「系統性下殺」與「個股基本面轉壞」，並針對所屬產業評估衝擊。')
bullet('相較一般聊天型 AI 的優勢：餵入即時行情與新聞，不憑空捏造。')
para('操作步驟', bold=True, color=ACCENT)
bullet('輸入代號（或從其他頁的「AI 明日預測」按鈕帶入）→ 自動分析。')
bullet('閱讀「今日消息面與盤勢連動」段落，掌握明日傾向。')
doc.add_paragraph()

# 8 AI Agent
h2('8. AI 自動化投資 Agent')
kv_table([('路徑', '/agent'), ('定位', '自動化的盤前 / 盤中 / 盤後 AI 操盤助理')])
para('重點功能', bold=True, color=ACCENT)
bullet('自動產生晨報、盤中提醒，並在收盤後彙整。')
bullet('持倉表逐檔「AI監測」開關，關閉者全程不呼叫 AI（省 Token 主力）。')
bullet('引け（13:45–14:15）推送「請登錄今日買賣」收盤提醒。')
bullet('AI 操盤建議綜合技術 + 基本（營收 / EPS）+ 籌碼（法人連續 / 借券）+ 消息 + 前夜美股。')
bullet('分析結果保存在瀏覽器，跳頁或重開分頁都還在。')
para('操作步驟', bold=True, color=ACCENT)
bullet('在持倉表登錄持股，逐檔決定是否開啟 AI 監測。')
bullet('查看 AI 自動產生的分析與候選清單；用開關控制成本。')
doc.add_paragraph()

# 9 AI 智能投資中心
h2('9. AI 智能投資中心')
kv_table([('路徑', '/ai'), ('定位', 'AI 功能的統一入口')])
para('重點功能', bold=True, color=ACCENT)
bullet('集中呈現 AI 相關功能與綜合建議的入口頁。')
bullet('清單列可直接點開個股詳情面板。')
para('操作步驟', bold=True, color=ACCENT)
bullet('從此頁快速進入預測、Agent、目標導向等 AI 功能。')
doc.add_paragraph()

# 10 目標導向操盤
h2('10. 目標導向操盤')
kv_table([('路徑', '/goal'), ('定位', '以獲利目標驅動，產生每日操盤總結')])
para('重點功能', bold=True, color=ACCENT)
bullet('設定獲利目標，AI 圍繞目標檢討進度。')
bullet('按鈕「產生今晚操盤總結」→ 一次彙整當日最佳策略、工作台結果與目標檢討。')
bullet('睡前若美股仍在盤中，標頭會標示「今夜美股（盤中）」。')
bullet('AI 討論對話會保存，重開仍在。')
para('操作步驟', bold=True, color=ACCENT)
bullet('先設定並啟用目標 → 收盤後按「產生今晚操盤總結」。')
bullet('閱讀總結並與 AI 對話討論隔日策略。')
doc.add_paragraph()

# 11 策略回測
h2('11. 策略回測')
kv_table([('路徑', '/backtest'), ('定位', '用歷史資料驗證策略績效')])
para('重點功能', bold=True, color=ACCENT)
bullet('將選股 / 操盤策略套用到過去區間，檢視報酬與表現。')
para('操作步驟', bold=True, color=ACCENT)
bullet('選擇策略與期間 → 執行回測 → 檢視績效結果。')
doc.add_paragraph()

# 12 策略工作台
h2('12. 策略工作台')
kv_table([('路徑', '/workbench'), ('定位', '自訂與組合策略條件')])
para('重點功能', bold=True, color=ACCENT)
bullet('自由組合技術 / 基本面條件，打造個人化策略。')
bullet('結果可供目標導向操盤的「今晚操盤總結」整併引用。')
para('操作步驟', bold=True, color=ACCENT)
bullet('設定條件 → 套用 → 查看符合清單，再點開個股詳情。')
doc.add_page_break()

# ===== 操盤強化層（2026-06 新增）=====
doc.add_page_break()
h1('三之二、操盤強化層（最新增強）')
para('以下功能是在原十二模組之上新增的「像投顧老師」強化層，主要強化「進場時機、何時賣、以及越用越懂你」三件事。多數整合在「AI 自動化 Agent」與「目標導向操盤」中自動運作。', color=GREY)
doc.add_paragraph()

h2('1. 大盤體質濾網')
bullet('用加權指數位置（站上/跌破月線 MA20、季線 MA60、月線方向）＋隔夜美股偏向，綜合判定多頭／震盪／空頭。')
bullet('大盤偏空時，自動把「買進」訊號靜音或降級，少在下跌段接刀；賣出/轉弱警示不受影響照常推。')

h2('2. 進場品質把關＋等回檔提示')
bullet('每次分析會判斷「此價位是否過熱」（乖離、自低點反彈幅度、RSI、KD 高檔）。')
bullet('偏熱時 AI 不叫你追高，而是明確給「等回到 X 元再分批進場」與停損價。')

h2('3. 進場守門員（買進前最後確認）')
bullet('起漲掃描頁可輸入代碼，一鍵回「現在是不是好買點／該等什麼價／停損設哪」，結合大盤體質、過熱程度、當日 K 線型態與主力出貨偵測。')

h2('4. K 線型態自動辨識')
bullet('自動標出當日觸發的買方型態：鎚子線、多頭吞噬、紅三兵、帶量突破新高、布林真突破、KD 低檔黃金交叉、月線翻揚等，並要 AI 據此說明「為何是買點」，不必自己看圖。')

h2('5. 主力出貨／快逃偵測')
bullet('綜合爆量收黑（量價背離）、跌破月線下彎、外資連續/雙賣超、當沖比飆高、融資增但法人賣、借券賣出大增、外資持股比例下降，分 high/med 警示。')
bullet('持倉盤中偵測到 high 即升級為「賣出」、med 為「減碼」，主動 LINE 推「疑似主力出貨、儘快出場」；要買進時偵測到 high 直接不建議。')

h2('6. 移動停利自動追蹤')
bullet('記住買進後的波段最高點，獲利部位從高點回落過深或跌破 5 日線時，盤中主動提醒鎖利（賺越多停利越收緊）。')

h2('7. 每日進場計畫＋隔日到價自動通知（核心）')
bullet('盤後（15:00 後每日一次，或手動「立即產生」）對「監測中但未持有」的個股，AI 研判明日是否進場，並用技術面定出買點區間／停損／停利／信心。')
bullet('隔日盤中即時價一進入買點區間，就 LINE 通知一次（到價即發、最無腦）；已到價不重複、已持有不再喊進。')

h2('8. 回測驗證選股勝率')
bullet('選股候選會用代表性起漲訊號回測，附「歷史勝率」；樣本足夠卻勝率太低的直接淘汰，落實「只推回測證明有效的」。')

h2('9. 個人化檢討＋AI 建議命中率')
bullet('用你的實際已實現帳本算勝率、賺賠比、獲利因子，並抓出壞習慣（賺小賠大、勝率偏低、未停損大賠），併入盤後總結。')
bullet('統計 AI 過去「買進建議」之後的方向命中率，幫你校準該信多少。')

h2('10. 散戶／法人籌碼結構')
bullet('以外資持股比例趨勢當法人/散戶結構代理（升＝散戶相對減少偏多、降＝散戶接手偏空），搭配融資餘額一起判讀。')
para('註：更精細的「集保大戶散戶分級」需 FinMind 付費級資料源；免費版以外資持股比例＋融資結構替代。', color=GREY, size=9.5)
doc.add_paragraph()

# ===== 常見問答 =====
h1('四、常見問題')
qa = [
    ('資料來源是即時的嗎？', '行情來自 yfinance（免費、免金鑰），為近即時資料；部分結果會短暫快取以加速與省資源。'),
    ('為什麼有些畫面在收盤 / 週末看起來怪怪的？', '非交易時段部分數值可能缺漏，系統已做防護，詳情面板仍可正常開啟。'),
    ('如何降低 AI 花費？', '用監測清單與持倉表的逐檔開關，只對重點股票開啟 AI；其餘停用或全部暫停。'),
    ('台股代號怎麼輸入？', '直接輸入四位數字（如 2330），系統自動補後綴並顯示中文名稱。'),
    ('停損該怎麼抓？', '低位階永動機法建議 -10% 可接受、-20% 為極限，跌破 -20% 系統會提示硬停損。'),
    ('「到價通知」怎麼運作？', '盤後系統對監測中、未持有的個股訂好明日買點區間；隔日盤中即時價一進入區間就 LINE 通知一次。前提：Agent 開關要開、LINE 與監測清單要設好。'),
    ('為什麼有時籌碼／法人資料顯示「未取得」？', '法人、融資、借券、月營收等來自 FinMind，免費額度有限；自動分析量大時可能暫時額度用罄（系統會優雅降級、不影響技術面與型態判斷），額度恢復後自動補回。要完整且穩定的籌碼資料可升級 FinMind 付費 token。'),
    ('一買就隔天跌怎麼辦？', '沒有系統能準測明天，重點是用大盤濾網、進場守門員、移動停利把勝率往自己偏、嚴設停損，長期靠紀律取勝。'),
]
for q, a in qa:
    p = doc.add_paragraph()
    rq = p.add_run('Q：' + q); rq.bold = True; rq.font.color.rgb = ACCENT; rq.font.size = Pt(11)
    pa = doc.add_paragraph()
    pa.add_run('A：' + a).font.size = Pt(10.5)
    doc.add_paragraph()

# 頁尾
foot = doc.add_paragraph()
foot.alignment = WD_ALIGN_PARAGRAPH.CENTER
rf = foot.add_run('— 文件結束 —')
rf.font.color.rgb = GREY
rf.font.size = Pt(10)

out = '/root/project/us_investment_stock_wang/智能投資分析平台_模組功能與操作手冊.docx'
doc.save(out)
print('SAVED', out)

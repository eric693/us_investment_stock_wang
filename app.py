from flask import Flask, render_template, jsonify, request
import yfinance as yf
import pandas as pd
import numpy as np
import warnings
import time
import threading
import json
import os
import requests as _requests
from xml.etree import ElementTree as ET
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout
warnings.filterwarnings('ignore')

app = Flask(__name__)

# ── 排程單一執行權（多 worker 下只讓一個 worker 跑背景排程）──────────────
# gunicorn 會啟動多個 worker，每個都會 import 本模組並嘗試啟動背景迴圈。
# 用檔案鎖確保「只有一個 worker」實際執行排程（避免重複推播、重複 API 花費）。
_scheduler_lock_fh = None
def _try_become_scheduler() -> bool:
    global _scheduler_lock_fh
    try:
        import fcntl
        fh = open('/tmp/us_inv_wang_scheduler.lock', 'w')
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _scheduler_lock_fh = fh   # 保留參考，鎖隨行程生命週期持有
        return True
    except Exception:
        return False

_IS_SCHEDULER = _try_become_scheduler()

# ── 去除 emoji（使用者偏好無 emoji；作為 system prompt 之外的保底）──────────
import re as _re
_EMOJI_RE = _re.compile(
    '[\U0001F000-\U0001FAFF\U00002600-\U000027BF\U00002B00-\U00002BFF'
    '\U0001F1E6-\U0001F1FF\U0000FE00-\U0000FE0F\U00002190-\U000021FF\U00002300-\U000023FF]',
    flags=_re.UNICODE)
def _strip_emoji(text: str) -> str:
    if not text:
        return text
    return _EMOJI_RE.sub('', text)

# ── Taiwan stock Chinese name cache ───────────────────────────────────
_tw_name_cache: dict[str, str] = {}
_tw_name_lock  = threading.Lock()

_TW_NAME_FILE = os.path.join(os.path.dirname(__file__), 'tw_names.json')


def _fetch_tw_names_finmind() -> dict:
    """FinMind TaiwanStockInfo：一次取全市場中文名（含 ETF），本機 IP 連得到。"""
    import urllib.request
    req = urllib.request.Request(
        'https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockInfo',
        headers={'User-Agent': 'Mozilla/5.0'})
    data = json.loads(urllib.request.urlopen(req, timeout=30).read())
    out = {}
    for r in data.get('data', []):
        sid = str(r.get('stock_id', '')).strip()
        nm  = (r.get('stock_name') or '').strip()
        if sid and nm and sid not in out:
            out[sid] = nm
    return out


def _fetch_tw_names_twse() -> dict:
    """TWSE/TPEX openapi 備援（注意：不含 ETF，且本機 IP 可能被 TWSE 封鎖）。"""
    import urllib.request
    out = {}
    for url in ('https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',
                'https://openapi.twse.com.tw/v1/exchangeReport/TPEX_STOCK_DAY_ALL'):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            for item in json.loads(urllib.request.urlopen(req, timeout=10).read()):
                code = item.get('Code') or item.get('SecuritiesCompanyCode', '')
                name = item.get('Name') or item.get('CompanyName', '')
                if code and name:
                    out[code] = name
        except Exception:
            pass
    return out


def _apply_tw_names(names: dict):
    global _tw_name_cache
    if not names:
        return
    with _tw_name_lock:
        _tw_name_cache = {**_tw_name_cache, **names}


def _load_tw_names():
    """背景維護台股中文名快取。

    來源優先序：本機快取檔（即時、離線可用）→ FinMind（含 ETF、本機 IP 連得到）
    → TWSE openapi（備援）。只有排程 worker 對外抓取並寫回快取檔，避免多 worker
    重複打 API；其餘 worker 定期重讀快取檔即可。"""
    try:
        if os.path.exists(_TW_NAME_FILE):
            with open(_TW_NAME_FILE, encoding='utf-8') as f:
                _apply_tw_names(json.load(f))
    except Exception:
        pass

    while True:
        if _IS_SCHEDULER:
            names = {}
            try:
                names = _fetch_tw_names_finmind()
            except Exception:
                names = {}
            if len(names) < 100:                     # FinMind 失敗或太少 → 補 TWSE
                try:
                    names = {**_fetch_tw_names_twse(), **names}
                except Exception:
                    pass
            if names:
                _apply_tw_names(names)
                try:
                    with _tw_name_lock:
                        snapshot = dict(_tw_name_cache)
                    with open(_TW_NAME_FILE, 'w', encoding='utf-8') as f:
                        json.dump(snapshot, f, ensure_ascii=False)
                except Exception:
                    pass
                time.sleep(86400)                    # 成功 → 一天刷新一次
            else:
                time.sleep(600)                      # 全失敗 → 10 分鐘後重試
        else:
            time.sleep(1800)                         # 非排程 worker：定期重讀快取檔
            try:
                if os.path.exists(_TW_NAME_FILE):
                    with open(_TW_NAME_FILE, encoding='utf-8') as f:
                        _apply_tw_names(json.load(f))
            except Exception:
                pass

threading.Thread(target=_load_tw_names, daemon=True).start()

def tw_cn_name(ticker: str, fallback: str) -> str:
    code = ticker.replace('.TW', '').replace('.TWO', '')
    with _tw_name_lock:
        return _tw_name_cache.get(code, fallback)

# ── Server-side Monitor ────────────────────────────────────────────────
MONITOR_FILE = os.path.join(os.path.dirname(__file__), 'monitor_config.json')
_monitor_lock = threading.Lock()

def _load_monitor_cfg():
    try:
        if os.path.exists(MONITOR_FILE):
            with open(MONITOR_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        pass
    return {'tickers': {}}

def _save_monitor_cfg(cfg):
    with open(MONITOR_FILE, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

def _split_for_line(text, size=4800):
    """LINE 單則上限 5000 字，超長就以行為界線切成多則（單行超長才硬切）。"""
    if not text:
        return ['']
    if len(text) <= size:
        return [text]
    chunks, cur = [], ''
    for line in text.split('\n'):
        while len(line) > size:        # 單行本身就超長，硬切
            if cur:
                chunks.append(cur); cur = ''
            chunks.append(line[:size]); line = line[size:]
        if len(cur) + len(line) + 1 > size:
            chunks.append(cur); cur = line
        else:
            cur = (cur + '\n' + line) if cur else line
    if cur:
        chunks.append(cur)
    return chunks


def _push_line_msg(token, user_id, text):
    # 超過單則上限就切成多則；一次 push 最多 5 則，避免整份報告因過長而完全推播失敗
    chunks = _split_for_line(text)
    try:
        for i in range(0, len(chunks), 5):
            batch = chunks[i:i + 5]
            _requests.post(
                'https://api.line.me/v2/bot/message/push',
                headers={'Content-Type': 'application/json',
                         'Authorization': f'Bearer {token}'},
                json={'to': user_id,
                      'messages': [{'type': 'text', 'text': c} for c in batch]},
                timeout=10,
            )
    except Exception as e:
        print(f'[Monitor] LINE error: {e}')

def _build_line_text(sig):
    return (
        f"【伺服器訊號】{sig.get('ticker','')} {sig.get('name','')}\n"
        f"動作: {sig.get('actionCn','')}\n"
        f"信心: {sig.get('confidence','-')}\n"
        f"時間: {sig.get('timestamp','')}\n"
        f"{sig.get('reason','')[:120]}\n"
        f"停損: {sig.get('trailingStop','')[:80]}"
    )


def _monitor_ai_advice(code, result, holding):
    """命中可動作訊號時，用 Claude 產生一段含現價/成本/具體買賣動作的綜合建議（僅台股）。"""
    try:
        cfg = _load_agent_cfg()
        api_key = _agent_api_key(cfg)
        if not api_key:
            return ''
        import anthropic
        data = _fetch_predict_data(code)
        if data.get('error'):
            return ''
        ctx = _agent_recommendation_text(data, holding)
        sig_line = (f"系統訊號：{result.get('actionCn','')}（信心 {result.get('confidence','-')}）"
                    f"— {result.get('reason','')}")
        ask = ('你是使用者的操盤顧問。請用繁體中文給精簡可執行的綜合建議，必須包含：'
               '(1)目前股價與（若有持倉）我的成本與損益；'
               '(2)【漲跌原因研判】結合技術面、法人，以及我提供的「市場與總經背景」「近期新聞標題」，'
               '說明近期為何漲/跌（大盤連動？題材？利空？），資料不足就誠實說明、不要捏造；'
               '(3)【影響評估】這是短期波動還是長期/結構性問題——若只是大盤連動或短期情緒、'
               '基本面未壞且虧損有限，請傾向建議續抱撐過並給觀察點，不要一跌就機械式叫賣；'
               '(4)現在該買進/續抱/減碼/賣出與具體價位。'
               '若標的為 ETF，請從淨值溢折價、配息與總經中長期角度判斷，不要只看短線技術指標。'
               '不要使用任何 emoji。\n\n'
               f'{sig_line}\n\n{ctx}')
        resp = anthropic.Anthropic(api_key=api_key).messages.create(
            model='claude-opus-4-8', max_tokens=1100,
            system=_agent_system_prompt(data),
            messages=[{'role': 'user', 'content': ask}],
        )
        return _strip_emoji(resp.content[0].text or '').strip()
    except Exception as e:
        print(f'[Monitor] ai advice {code}: {e}')
        return ''


def _build_monitor_message(ticker, result, price):
    """組監測推播：技術訊號標頭 + 持倉成本/現價/損益 + AI 綜合建議。"""
    msg  = _build_line_text(result)
    code = ticker.upper().replace('.TWO', '').replace('.TW', '')

    holding = None
    try:
        holding = next((h for h in _load_agent_cfg().get('holdings', [])
                        if h.get('code', '').strip().upper() == code), None)
    except Exception:
        holding = None

    if holding and price:
        buy_p  = safe_float(holding.get('buy_price', 0))
        shares = safe_float(holding.get('shares', 0))
        pnl    = (price - buy_p) / buy_p * 100 if buy_p else 0
        msg += (f"\n\n你的持倉：成本 {buy_p:g} 元 / {shares:g} 股 / "
                f"現價 {price:.2f} 元 / 未實現損益 {pnl:+.2f}%")

    # AI 綜合建議：僅台股（_fetch_predict_data 為台股資料源），命中時才呼叫以控制花費
    if ticker.upper().endswith(('.TW', '.TWO')):
        advice = _monitor_ai_advice(code, result, holding)
        if advice:
            msg += f"\n\n【AI 綜合建議】\n{advice}"
    return msg


def _run_server_scan():
    with _monitor_lock:
        cfg = _load_monitor_cfg()
    tickers_cfg = cfg.get('tickers', {})
    if not tickers_cfg:
        return
    if cfg.get('monitor_paused'):          # 總開關：暫停全部監測（省 token）
        return
    now_str = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M')
    for ticker, settings in list(tickers_cfg.items()):
        try:
            if not settings.get('enabled', True):   # 個別開關：關閉的標的完全不掃描、不呼叫 AI
                continue
            profile = settings.get('profile', 'aggressive')
            stock = yf.Ticker(ticker)
            info = stock.info
            price = safe_float(info.get('currentPrice', info.get('regularMarketPrice', 0)))
            if price <= 0:
                continue
            name = tw_cn_name(ticker, info.get('shortName', info.get('longName', ticker)))
            if profile == 'aggressive':
                result = _aggressive_signal(stock, ticker, price, name)
            elif profile == 'perpetual':
                code_p = ticker.replace('.TW', '').replace('.TWO', '')
                hold_p = next((h for h in _load_agent_cfg().get('holdings', [])
                               if h.get('code', '').strip().upper() == code_p), None)
                result = _perpetual_signal(stock, ticker, price, name, hold_p)
            else:
                result = _steady_signal(stock, ticker, price, name)
            action = result.get('action', 'WAIT')
            should_notify = False
            line_token = line_user_id = ''
            with _monitor_lock:
                cfg2 = _load_monitor_cfg()
                if ticker not in cfg2['tickers']:
                    continue
                cfg2['tickers'][ticker]['last_signal'] = result
                cfg2['tickers'][ticker]['last_scan'] = now_str
                entry        = cfg2['tickers'][ticker]
                line_token   = entry.get('line_token', '')
                line_user_id = entry.get('line_user_id', '')
                last_action  = entry.get('last_notify_action', '')
                last_notify  = entry.get('last_notify_time', '')
                # 去重複：同一個動作不要每 30 分鐘重發，只有「動作改變」才再次通知
                # （例如 WAIT→SELL、BUY→AVOID）。同一動作持續時保持安靜。
                action_changed = (action != last_action)
                # 防抖：訊號在臨界值附近來回跳動時，60 分鐘內不重複推播
                cooldown_ok  = (not last_notify or
                    (pd.Timestamp.now(tz='Asia/Taipei') -
                     pd.Timestamp(last_notify, tz='Asia/Taipei')).total_seconds() > 3600)
                # 降低機械式雜訊：只有「買進」類訊號才要求中/高信心（避免一跌就嘮叨叫買）；
                # 賣出/轉弱(SELL/AVOID)是風險警示，照原本去重＋冷卻規則照常推，不被信心過濾漏掉。
                conf_ok = (action != 'BUY'
                           or str(result.get('confidence', '-')).strip() in ('高', '中'))
                # 只在該標的所屬市場的交易時段內推播（避免半夜/收盤後狂發）；
                # 買進與賣出/轉弱（SELL、AVOID）都通知，讓使用者有賣有買。
                if (action in ('BUY', 'SELL', 'AVOID') and action_changed and cooldown_ok and conf_ok
                        and line_token and line_user_id and _ticker_session_open(ticker)):
                    cfg2['tickers'][ticker]['last_notify_time']   = now_str
                    cfg2['tickers'][ticker]['last_notify_action'] = action
                    should_notify = True
                _save_monitor_cfg(cfg2)
            # 組訊息＋AI 綜合建議放在鎖外（含網路 I/O），避免阻塞其他標的掃描
            if should_notify:
                msg = _build_monitor_message(ticker, result, price)
                _push_line_msg(line_token, line_user_id, msg)
        except Exception as e:
            print(f'[Monitor] scan {ticker}: {e}')

def _server_scan_loop():
    if not _IS_SCHEDULER:
        return
    time.sleep(15)  # let app finish startup
    while True:
        try:
            _run_server_scan()
        except Exception as e:
            print(f'[Monitor] loop error: {e}')
        time.sleep(300)  # 5 minutes

threading.Thread(target=_server_scan_loop, daemon=True).start()

# ── TTL Cache（L1 記憶體 + L2 SQLite 跨 worker 共用）─────────────────────
# gunicorn 多 worker 各有自己的記憶體，L2 讓昂貴查詢只需被某一個 worker 抓一次。
# 所有 L2 操作都包在 try/except，任何問題都會自動退回「純記憶體」＝原本行為。
import sqlite3 as _sqlite3
_CACHE = {}
_cache_lock = threading.Lock()
_CACHE_DB = os.path.join(os.path.dirname(__file__), 'cache.sqlite')

def _cache_db_init():
    try:
        conn = _sqlite3.connect(_CACHE_DB, timeout=2)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('CREATE TABLE IF NOT EXISTS cache (k TEXT PRIMARY KEY, v TEXT, exp REAL)')
        conn.commit(); conn.close()
    except Exception:
        pass
_cache_db_init()

def _cache_get(key):
    e = _CACHE.get(key)
    if e and time.time() < e['t']:
        return e['v']
    # L2：SQLite（跨 worker 共用）
    try:
        conn = _sqlite3.connect(_CACHE_DB, timeout=1)
        row  = conn.execute('SELECT v, exp FROM cache WHERE k=?', (key,)).fetchone()
        conn.close()
        if row and time.time() < row[1]:
            val = json.loads(row[0])
            _CACHE[key] = {'v': val, 't': row[1]}   # 回填 L1
            return val
    except Exception:
        pass
    return None

def _cache_set(key, val, ttl=300):
    now = time.time(); exp = now + ttl
    with _cache_lock:
        _CACHE[key] = {'v': val, 't': exp}
        # 順手清掉過期項目，避免長跑的 gunicorn 下無限增長
        if len(_CACHE) > 50:
            for k in [k for k, e in _CACHE.items() if e['t'] <= now]:
                _CACHE.pop(k, None)
    # L2：只寫可乾淨序列化的值；不可序列化就僅留 L1（等同原行為）
    try:
        payload = json.dumps(val, ensure_ascii=False)
    except (TypeError, ValueError):
        return
    try:
        conn = _sqlite3.connect(_CACHE_DB, timeout=1)
        conn.execute('INSERT OR REPLACE INTO cache (k, v, exp) VALUES (?, ?, ?)', (key, payload, exp))
        if now % 20 < 1:   # 偶爾清掉過期列
            conn.execute('DELETE FROM cache WHERE exp < ?', (now,))
        conn.commit(); conn.close()
    except Exception:
        pass

# ── Helpers ───────────────────────────────────────────────────────
def safe_float(v, default=0.0):
    try:
        if v is None: return default
        f = float(v)
        return default if (np.isnan(f) or np.isinf(f)) else f
    except Exception:
        return default

def last_valid(series, default=0.0):
    """Return last non-NaN value from a pandas Series (handles today's NaN for TW stocks)."""
    try:
        s = series.dropna()
        return safe_float(s.iloc[-1]) if len(s) else default
    except Exception:
        return default

def safe_int(v, default=0):
    try:
        return int(safe_float(v))
    except Exception:
        return default

# ── Technical Indicators ──────────────────────────────────────────
def calc_macd(close, fast=12, slow=26, sig=9):
    e1 = close.ewm(span=fast, adjust=False).mean()
    e2 = close.ewm(span=slow, adjust=False).mean()
    macd = e1 - e2
    signal = macd.ewm(span=sig, adjust=False).mean()
    return macd, signal, macd - signal

def calc_rsi(close, period=14):
    d = close.diff()
    gain = d.clip(lower=0).rolling(period).mean()
    loss = (-d.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def calc_bollinger(close, period=20, std_dev=2):
    ma  = close.rolling(period).mean()
    std = close.rolling(period).std()
    return ma + std_dev * std, ma, ma - std_dev * std

def calc_gmma(close):
    """Guppy Multiple Moving Average — returns (short_vals, long_vals) as lists"""
    short_periods = [3, 5, 8, 10, 12, 15]
    long_periods  = [30, 35, 40, 45, 50, 60]
    short_vals = [safe_float(close.ewm(span=p, adjust=False).mean().iloc[-1]) for p in short_periods]
    long_vals  = [safe_float(close.ewm(span=p, adjust=False).mean().iloc[-1]) for p in long_periods]
    return short_vals, long_vals


# ── Signal Engines ────────────────────────────────────────────────────
def _aggressive_signal(stock, ticker, price, name):
    """激進爆發型：5分K帶量突破 + MACD 放大"""
    hist = stock.history(period='5d', interval='5m')
    if hist.empty or len(hist) < 30:
        return _signal_wait(ticker, name, price, 'aggressive', '盤中資料不足，無法判斷')

    close  = hist['Close']
    volume = hist['Volume']

    # MACD on 5-min bars
    macd_s, sig_s, hist_s = calc_macd(close)
    hist_val  = safe_float(hist_s.iloc[-1])
    hist_prev = safe_float(hist_s.iloc[-2]) if len(hist_s) > 1 else 0
    macd_bullish = hist_val > 0 and hist_val > hist_prev

    # 20-bar SMA for stop loss reference
    ma20 = close.rolling(20).mean()
    ma20_val = safe_float(ma20.iloc[-1])

    # Breakout: price > max of last 20 bars (excluding current)
    recent_high = safe_float(hist['High'].iloc[-21:-1].max()) if len(hist) >= 21 else safe_float(hist['High'].max())
    is_breakout = price > recent_high

    # Volume: current > 1.5x rolling 20-bar mean
    avg_vol  = safe_float(volume.rolling(20).mean().iloc[-1])
    curr_vol = safe_float(volume.iloc[-1])
    vol_ratio = curr_vol / avg_vol if avg_vol > 0 else 0
    vol_confirmed = vol_ratio >= 1.5

    stop_loss = round(ma20_val * 0.985, 2)
    bull_count = sum([is_breakout, vol_confirmed, macd_bullish])

    if bull_count >= 2:
        action, action_cn = 'BUY', '動能追擊！建議買進'
        conf = '高' if bull_count == 3 else '中'
        reason = (f'5分K帶量突破盤整區（量比 {vol_ratio:.1f}x），短線動能強勁。'
                  f'MACD 柱狀翻紅放大，此為高勝率突破訊號，請注意控制部位風險。')
    elif is_breakout and not vol_confirmed:
        action, action_cn = 'WATCH', '盤整突破！量能待確認'
        conf = '低'
        reason = (f'價格突破近期高點 {recent_high:.2f} 元，但量能不足（量比 {vol_ratio:.1f}x < 1.5x）。'
                  f'建議等待放量確認再進場，避免假突破。')
    else:
        action, action_cn = 'WAIT', '持續觀望，尚未觸發'
        conf = '-'
        reason = (f'未出現帶量突破信號。近期高點 {recent_high:.2f} 元，'
                  f'當前量比 {vol_ratio:.1f}x，MACD {"多頭" if hist_val > 0 else "空頭"}。')

    return {
        'ticker': ticker, 'name': name, 'price': round(price, 2),
        'profile': 'aggressive', 'action': action, 'actionCn': action_cn,
        'confidence': conf, 'reason': reason, 'stopLoss': stop_loss,
        'trailingStop': f'跌破 15分K MA20（{ma20_val:.2f} 元）時建議獲利了結',
        'details': {
            'breakout': is_breakout, 'breakoutLevel': round(recent_high, 2),
            'volRatio': round(vol_ratio, 2), 'volConfirmed': vol_confirmed,
            'macdBullish': macd_bullish, 'macdHist': round(hist_val, 4),
            'ma20': round(ma20_val, 2),
        },
        'timeframe': '5分K',
        'timestamp': pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M'),
    }


def _steady_signal(stock, ticker, price, name):
    """穩健保守型：日K GMMA 支撐 + MACD 底部轉強"""
    hist = stock.history(period='6mo', interval='1d')
    if hist.empty or len(hist) < 60:
        return _signal_wait(ticker, name, price, 'steady', '歷史資料不足，無法判斷')

    close  = hist['Close']
    volume = hist['Volume']

    # GMMA
    short_vals, long_vals = calc_gmma(close)
    long_min = min(long_vals); long_max = max(long_vals)

    # 「逢低佈局」只有股價真的貼近長期均線支撐區時才成立；
    # 原本用 above_support=price>long_min，會讓股價在高檔時也被當成逢低（00918 的問題）。
    near_support  = long_min * 0.97 <= price <= long_max * 1.05
    broke_support = price < long_min * 0.97
    # 距長期均線帶頂的乖離：>15% 視為已遠離支撐、追高風險高
    ext_pct       = (price - long_max) / long_max * 100 if long_max else 0
    extended      = ext_pct > 15

    # MACD daily
    macd_s, sig_s, hist_s = calc_macd(close)
    macd_val   = safe_float(macd_s.iloc[-1])
    macd_prev  = safe_float(macd_s.iloc[-2])
    sig_val    = safe_float(sig_s.iloc[-1])
    sig_prev   = safe_float(sig_s.iloc[-2])
    hist_val   = safe_float(hist_s.iloc[-1])
    hist_prev  = safe_float(hist_s.iloc[-2])

    golden_cross  = macd_val > sig_val and macd_prev <= sig_prev
    macd_turning  = hist_val > hist_prev and hist_val < 0      # improving from negative
    macd_positive = hist_val > 0

    # Volume shrinking (縮量打底)
    recent_vol = safe_float(volume.iloc[-5:].mean())
    older_vol  = safe_float(volume.iloc[-20:-5].mean())
    vol_shrink = recent_vol < older_vol * 0.85 if older_vol > 0 else False

    # RSI
    rsi_val = safe_float(calc_rsi(close).iloc[-1])
    oversold = rsi_val < 40

    stop_loss = round(long_min * 0.96, 2)
    overbought = rsi_val > 75

    if near_support and (golden_cross or macd_positive) and vol_shrink:
        action, action_cn = 'BUY', '安全打底！逢低佈局'
        conf = '高' if (golden_cross and vol_shrink) else '中'
        reason = (f'股價 {price:.2f} 元回測 GMMA 長期均線支撐（{long_min:.2f}~{long_max:.2f} 元）不破，'
                  f'量縮打底，MACD {"出現黃金交叉" if golden_cross else "底部轉強"}，'
                  f'適合做中長線的資金投入。')
    elif broke_support:
        action, action_cn = 'AVOID', '跌破支撐，趨勢轉弱'
        conf = '-'
        reason = (f'股價 {price:.2f} 元跌破 GMMA 長期均線支撐（{long_min:.2f} 元），趨勢轉弱，'
                  f'持有者宜減碼防守，等待重新站回長期均線後再考慮進場。')
    elif extended and overbought:
        action, action_cn = 'SELL', '漲多過熱，分批了結'
        conf = '中'
        reason = (f'股價 {price:.2f} 元已高出長期均線帶頂（{long_max:.2f} 元）約 {ext_pct:.0f}%，'
                  f'RSI {rsi_val:.0f} 過熱，並非逢低區。持有者可分批獲利了結，空手者勿追高，'
                  f'等拉回貼近 {long_max:.2f} 元附近再評估。')
    elif near_support and (macd_turning or oversold):
        action, action_cn = 'WATCH', '接近支撐！持續觀察'
        conf = '低'
        reason = (f'股價逼近 GMMA 長期均線支撐區（{long_min:.2f}~{long_max:.2f} 元）。'
                  f'{"RSI " + str(round(rsi_val, 0)) + " 超賣，" if oversold else ""}'
                  f'MACD 底部出現轉強跡象，若量縮確認後可逢低佈局。')
    elif extended:
        action, action_cn = 'WAIT', '已遠離支撐，不宜追高'
        conf = '-'
        reason = (f'股價 {price:.2f} 元高出長期均線帶頂（{long_max:.2f} 元）約 {ext_pct:.0f}%，'
                  f'位階偏高，非逢低佈局時機，建議等拉回支撐區再評估。')
    else:
        action, action_cn = 'WAIT', '持續觀望，尚未觸發'
        conf = '-'
        reason = (f'股價 {price:.2f} 元，GMMA 長期支撐 {long_min:.2f}~{long_max:.2f} 元。'
                  f'未達最佳進場條件，建議尾盤再次確認日K型態。')

    return {
        'ticker': ticker, 'name': name, 'price': round(price, 2),
        'profile': 'steady', 'action': action, 'actionCn': action_cn,
        'confidence': conf, 'reason': reason, 'stopLoss': stop_loss,
        'trailingStop': f'跌破前波大頸線（{stop_loss:.2f} 元）才建議停損，給予較寬防守空間',
        'details': {
            'gmmaLongMin': round(long_min, 2), 'gmmaLongMax': round(long_max, 2),
            'gmmaShortMin': round(min(short_vals), 2),
            'nearSupport': near_support, 'brokeSupport': broke_support,
            'goldenCross': golden_cross, 'macdTurning': macd_turning,
            'volShrink': vol_shrink, 'rsi': round(rsi_val, 1),
        },
        'timeframe': '日K',
        'timestamp': pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M'),
    }


def _signal_wait(ticker, name, price, profile, reason):
    return {
        'ticker': ticker, 'name': name, 'price': round(price, 2),
        'profile': profile, 'action': 'WAIT', 'actionCn': '持續觀望',
        'confidence': '-', 'reason': reason, 'stopLoss': 0,
        'trailingStop': '', 'details': {},
        'timeframe': '5分K' if profile == 'aggressive' else '日K',
        'timestamp': pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M'),
    }

def calc_returns(hist):
    c   = hist['Close']
    cur = safe_float(c.iloc[-1])
    out = {}
    for label, days in [('1W', 5), ('1M', 21), ('3M', 63), ('6M', 126), ('1Y', 252)]:
        if len(c) > days:
            past = safe_float(c.iloc[-(days + 1)])
            out[label] = round((cur / past - 1) * 100, 2) if past else None
        else:
            out[label] = None
    return out

def get_levels(hist):
    h, l = hist['High'], hist['Low']
    return {
        'resistance2': round(safe_float(h.max()), 2),
        'resistance1': round(safe_float(h.rolling(20).max().iloc[-1]), 2),
        'support1':    round(safe_float(l.rolling(20).min().iloc[-1]), 2),
        'support2':    round(safe_float(l.rolling(60).min().iloc[-1]), 2),
    }

# ── Analysis Generators ───────────────────────────────────────────
def gen_conclusions(price, ma5, ma20, ma60, macd, dea, rsi, vol_ratio):
    out = []
    if price > ma5 > ma20 > ma60:
        out.append({'type': 'star',     'text': '三均線多頭完美排列，趨勢強勢向上'})
    elif price > ma20 > ma60:
        out.append({'type': 'positive', 'text': '股價站穩 MA20 均線，中期趨勢偏多'})
    elif price < ma60:
        out.append({'type': 'negative', 'text': '股價跌破 MA60 均線，趨勢偏空需謹慎'})
    else:
        out.append({'type': 'neutral',  'text': '股價在均線間整理，方向待確認'})

    if macd > dea and macd > 0:
        out.append({'type': 'positive', 'text': f'MACD 金叉且在零軸上方，多頭動能強勁（DIF: {macd:.2f}）'})
    elif macd > dea:
        out.append({'type': 'positive', 'text': 'MACD 低位出現金叉，技術面轉強訊號'})
    else:
        out.append({'type': 'warning',  'text': 'MACD 死叉，短期動能偏弱，觀望為主'})

    if vol_ratio >= 2.0:
        out.append({'type': 'positive', 'text': f'成交量爆量（均量 {vol_ratio:.1f}x），主力資金大幅介入'})
    elif vol_ratio >= 1.5:
        out.append({'type': 'positive', 'text': f'成交量放大（均量 {vol_ratio:.1f}x），資金積極流入'})
    else:
        out.append({'type': 'neutral',  'text': f'成交量正常（均量 {vol_ratio:.1f}x），市場觀望情緒'})

    if rsi > 80:
        out.append({'type': 'warning',  'text': f'RSI {rsi:.0f} 極度超買，注意獲利了結回調'})
    elif rsi > 70:
        out.append({'type': 'warning',  'text': f'RSI {rsi:.0f} 進入超買區，短線謹慎追高'})
    elif rsi < 30:
        out.append({'type': 'positive', 'text': f'RSI {rsi:.0f} 超賣區，技術性反彈機會提升'})
    elif 50 <= rsi <= 70:
        out.append({'type': 'positive', 'text': f'RSI {rsi:.0f} 健康多頭區間，上漲動能充足'})
    else:
        out.append({'type': 'neutral',  'text': f'RSI {rsi:.0f} 中性區間，等待方向確認'})

    return out[:5]

def gen_catalysts(price, ma5, ma20, ma60, macd, dea, rsi, vol_ratio, week52h, info):
    cats = []

    # ── Technical catalysts (objective, data-driven) ──
    if price >= week52h * 0.97:
        cats.append({'num': 1, 'text': '突破或接近52週高點，強勢創高訊號',
                     'sub': '價格創歷史新高，市場認可度顯著提升，突破後動能往往延續'})
    if macd > dea and macd > 0:
        cats.append({'num': len(cats)+1, 'text': 'MACD 金叉且在零軸上方，多頭動能確立',
                     'sub': '短中期均偏多，技術訊號轉強，趨勢延續性高'})
    if vol_ratio >= 1.5:
        cats.append({'num': len(cats)+1, 'text': f'成交量放大 {vol_ratio:.1f}x，資金積極進場',
                     'sub': '量能配合價格上漲，籌碼結構改善，機構介入意願增強'})
    if price > ma5 > ma20 > ma60:
        cats.append({'num': len(cats)+1, 'text': '均線多頭排列完整，中長期趨勢向上',
                     'sub': '短中長期均線同向支撐，回撤即布局機會，趨勢延續性高'})
    target = safe_float(info.get('targetMeanPrice', 0))
    if target > price * 1.1:
        upside = (target / price - 1) * 100
        cats.append({'num': len(cats)+1,
                     'text': f'分析師共識目標 ${target:.2f}（潛在漲幅 +{upside:.0f}%）',
                     'sub': '華爾街分析師看好後市，平均目標價相對現價仍有顯著上漲空間'})

    # ── Fundamental catalysts — only include when data is genuinely positive ──
    sector     = info.get('sector', '')
    industry   = info.get('industry', '')
    div_yield  = safe_float(info.get('dividendYield', 0))  # already in % format
    inst_pct   = round(safe_float(info.get('heldPercentInstitutions', 0)) * 100, 1)
    rev_growth = round(safe_float(info.get('revenueGrowth', 0)) * 100, 1)
    fwd_pe     = safe_float(info.get('forwardPE', 0))
    pe         = safe_float(info.get('trailingPE', 0))
    rec        = info.get('recommendationKey', '')
    beta       = safe_float(info.get('beta', 0))

    extras = []

    # Sector — neutral, fact-based description (no macro timing claims)
    if 'Semiconductor' in industry or 'Technology' in sector:
        extras.append({'text': 'AI / 半導體長期成長邏輯清晰，產業地位穩固',
                       'sub':  '受惠全球算力需求擴張，數據中心與終端裝置需求雙驅動'})
    elif 'Health' in sector:
        extras.append({'text': '醫療健康產業具防禦特性，長期成長確定',
                       'sub':  '老齡化社會驅動醫療支出長期成長，研發管線具催化潛力'})
    elif 'Financial' in sector:
        extras.append({'text': '金融業務多元化，利差與手續費收入組合穩定',
                       'sub':  '業務橫跨零售銀行、財富管理、資本市場，收入來源分散'})
    elif 'Energy' in sector:
        extras.append({'text': '能源公司現金流充沛，資本回報計畫積極',
                       'sub':  '高自由現金流支撐股票回購與股息計畫，股東回報率具競爭力'})
    elif 'Consumer' in sector:
        extras.append({'text': '品牌護城河穩固，定價能力強',
                       'sub':  '高品牌忠誠度保護利潤率，消費者支出需求具韌性'})
    elif 'Communication' in sector:
        extras.append({'text': '平台效應顯著，用戶黏著度高',
                       'sub':  '數位廣告市場份額持續擴大，AI 整合提升商業化效率'})

    # Revenue growth — only if genuinely positive
    if rev_growth > 20:
        extras.append({'text': f'營收年增 {rev_growth:.0f}%，業績高速成長',
                       'sub':  '高速成長印證商業模式可行，機構法人持續上調目標價'})
    elif rev_growth > 5:
        extras.append({'text': f'營收成長 {rev_growth:.0f}%，基本面持續改善',
                       'sub':  '成長軌道持續，盈利品質穩定，估值有基本面支撐'})
    elif rev_growth > 0:
        extras.append({'text': f'營收小幅成長 {rev_growth:.1f}%，業績逐步回穩',
                       'sub':  '成長動能初步回升，若下季加速將成強力催化劑'})
    # rev_growth <= 0：不加入，負成長不是催化劑

    # Institutional holdings — only if meaningfully high
    if inst_pct >= 60:
        extras.append({'text': f'機構持股 {inst_pct:.0f}%，主力資金深度佈局',
                       'sub':  '大型機構長線持有，籌碼結構穩定，護盤意願強'})
    elif inst_pct >= 40:
        extras.append({'text': f'機構持股 {inst_pct:.0f}%，法人籌碼穩固',
                       'sub':  '機構持倉比重高，短期賣壓有限，股價支撐較強'})
    # inst_pct < 40：不加入，低機構持股不是催化劑

    # Dividend — only if actually paying
    if div_yield >= 4:
        extras.append({'text': f'高殖利率 {div_yield:.1f}%，現金流收益豐厚',
                       'sub':  '穩定高股息提供下跌緩衝，吸引退休金與長線存股資金'})
    elif div_yield >= 1:
        extras.append({'text': f'殖利率 {div_yield:.1f}%，股東回饋穩定',
                       'sub':  '定期現金股利顯示公司財務健康，長線資金偏好'})
    # div_yield < 1：不加入，無配息不是催化劑

    # Valuation — only if forward PE is attractive
    if 0 < fwd_pe < 18 and (pe <= 0 or fwd_pe < pe * 0.85):
        extras.append({'text': f'預估本益比 {fwd_pe:.1f}x，估值具吸引力',
                       'sub':  '前瞻本益比相對合理，盈利成長空間尚未完全被市場定價'})
    elif rec in ('buy', 'strong_buy'):
        extras.append({'text': '分析師評級偏向買進，市場共識看好後市',
                       'sub':  '主流券商維持或上調評級，基本面與技術面催化劑逐步匯聚'})

    # Fill to 4 using genuine positives
    for item in extras:
        if len(cats) >= 4:
            break
        cats.append({'num': len(cats) + 1, **item})

    # Fallback: add factual items if still short (avoid fake positives)
    if len(cats) < 2:
        ref_pe = fwd_pe if fwd_pe > 0 else pe
        if ref_pe > 0:
            cats.append({'num': len(cats)+1,
                         'text': f'本益比 {ref_pe:.1f}x，與同業比較評估合理性',
                         'sub':  '建議對照產業平均本益比，判斷當前估值是否具佈局價值'})
        if beta > 0 and len(cats) < 2:
            cats.append({'num': len(cats)+1,
                         'text': f'Beta {beta:.2f}，{"波動低於大盤，適合穩健布局" if beta < 1 else "波動高於大盤，適合積極型投資人"}',
                         'sub':  '波動性數據有助於評估個股在投資組合中的風險貢獻'})

    return cats[:4]

def gen_investment_value(price, ma5, ma20, ma60, macd_v, dea_v, rsi_v,
                         pe, fwd_pe, roe, profit_margin, rev_growth, eps_growth,
                         beta, debt_equity, vol_ratio):
    s = {}
    # Momentum (0-4)
    if price > ma5 > ma20 > ma60 and macd_v > dea_v and macd_v > 0:
        s['momentum'] = 4
    elif price > ma5 > ma20 > ma60:
        s['momentum'] = 3
    elif price > ma20 > ma60:
        s['momentum'] = 2
    elif price > ma60:
        s['momentum'] = 1
    else:
        s['momentum'] = 0

    # Valuation (0-4)
    ref_pe = fwd_pe if fwd_pe and fwd_pe > 0 else (pe if pe and pe > 0 else 0)
    if   ref_pe <= 0:  s['valuation'] = 2
    elif ref_pe < 15:  s['valuation'] = 4
    elif ref_pe < 25:  s['valuation'] = 3
    elif ref_pe < 40:  s['valuation'] = 2
    elif ref_pe < 60:  s['valuation'] = 1
    else:              s['valuation'] = 0

    # Growth (0-4)
    avg_g = (rev_growth + eps_growth) / 2 if eps_growth else rev_growth
    if   avg_g > 30: s['growth'] = 4
    elif avg_g > 15: s['growth'] = 3
    elif avg_g > 5:  s['growth'] = 2
    elif avg_g > 0:  s['growth'] = 1
    else:            s['growth'] = 0

    # Financial health (0-4)
    h = 2
    if roe > 20:           h += 1
    elif roe < 0:          h -= 1
    if profit_margin > 15: h += 1
    elif profit_margin < 0:h -= 1
    if debt_equity > 200:  h -= 1
    elif debt_equity < 50: h += 1
    s['health'] = max(0, min(4, h))

    def grade(v):
        return 'A+' if v >= 4 else 'A' if v == 3 else 'B' if v == 2 else 'C' if v == 1 else 'D'

    total = sum(s.values()); pct = total / 16
    if   pct >= 0.75: sig, sig_cn, sig_cls = 'STRONG BUY', '強烈買入', 'sv-strong-buy'
    elif pct >= 0.60: sig, sig_cn, sig_cls = 'BUY',        '買入',     'sv-buy'
    elif pct >= 0.45: sig, sig_cn, sig_cls = 'HOLD',       '持有',     'sv-hold'
    elif pct >= 0.30: sig, sig_cn, sig_cls = 'CAUTION',    '觀望',     'sv-caution'
    else:             sig, sig_cn, sig_cls = 'AVOID',      '迴避',     'sv-avoid'

    strengths, weaknesses = [], []
    if s['momentum'] >= 3:  strengths.append('技術趨勢強勁，均線多頭排列完整')
    if s['growth']   >= 3:  strengths.append(f'高速成長，營收 +{rev_growth:.0f}% / EPS +{eps_growth:.0f}%')
    if s['valuation']>= 3:  strengths.append(f'估值合理，預期本益比 {ref_pe:.0f}x 具吸引力')
    if s['health']   >= 3:  strengths.append(f'財務健康，ROE {roe:.0f}%，淨利率 {profit_margin:.0f}%')
    if vol_ratio >= 1.5:    strengths.append(f'量能放大（{vol_ratio:.1f}x），機構積極介入')
    if rsi_v < 40:          strengths.append(f'RSI {rsi_v:.0f} 低檔，技術性反彈空間大')

    if s['valuation'] <= 1 and ref_pe > 0: weaknesses.append(f'估值偏高（本益比 {ref_pe:.0f}x），需業績持續兌現')
    if rsi_v > 70:          weaknesses.append(f'RSI {rsi_v:.0f} 超買，短線追高風險')
    if s['growth'] <= 1:    weaknesses.append('成長動能偏弱，需觀察業績轉機')
    if debt_equity > 150:   weaknesses.append(f'負債比 {debt_equity:.0f}% 偏高，財務槓桿風險')
    if s['momentum'] <= 1:  weaknesses.append('趨勢偏弱，建議等待均線翻多再布局')

    return {
        'signal':   sig,
        'signalCn': sig_cn,
        'signalCls': sig_cls,
        'score':    round(pct * 100),
        'grades': {
            'momentum':  grade(s['momentum']),
            'valuation': grade(s['valuation']),
            'growth':    grade(s['growth']),
            'health':    grade(s['health']),
        },
        'strengths':  strengths[:3],
        'weaknesses': weaknesses[:2],
    }

def gen_etf_investment_value(price, ma5, ma20, ma60, macd_v, dea_v, rsi_v,
                             div_yield, expense_ratio, total_assets, vol_ratio,
                             ytd_return=0, three_yr=0):
    s = {}
    # Momentum (0-4)
    if price > ma5 > ma20 > ma60 and macd_v > dea_v:
        s['momentum'] = 4
    elif price > ma5 > ma20 > ma60:
        s['momentum'] = 3
    elif price > ma20 > ma60:
        s['momentum'] = 2
    elif price > ma60:
        s['momentum'] = 1
    else:
        s['momentum'] = 0

    # Dividend (0-4)
    if   div_yield >= 6:  s['dividend'] = 4
    elif div_yield >= 4:  s['dividend'] = 3
    elif div_yield >= 2:  s['dividend'] = 2
    elif div_yield >= 1:  s['dividend'] = 1
    else:                 s['dividend'] = 0

    # Cost (0-4) — lower expense ratio is better
    if   expense_ratio == 0:    s['cost'] = 2
    elif expense_ratio < 0.2:   s['cost'] = 4
    elif expense_ratio < 0.5:   s['cost'] = 3
    elif expense_ratio < 1.0:   s['cost'] = 2
    elif expense_ratio < 1.5:   s['cost'] = 1
    else:                       s['cost'] = 0

    # Scale (0-4) — larger AUM means more liquidity
    if   total_assets >= 500:   s['scale'] = 4
    elif total_assets >= 100:   s['scale'] = 3
    elif total_assets >= 30:    s['scale'] = 2
    elif total_assets >= 5:     s['scale'] = 1
    else:                       s['scale'] = 0

    def grade(v):
        return 'A+' if v >= 4 else 'A' if v == 3 else 'B' if v == 2 else 'C' if v == 1 else 'D'

    total = sum(s.values()); pct = total / 16
    if   pct >= 0.75: sig, sig_cn, sig_cls = 'STRONG BUY', '強烈買入', 'sv-strong-buy'
    elif pct >= 0.60: sig, sig_cn, sig_cls = 'BUY',        '買入',     'sv-buy'
    elif pct >= 0.45: sig, sig_cn, sig_cls = 'HOLD',       '持有',     'sv-hold'
    elif pct >= 0.30: sig, sig_cn, sig_cls = 'CAUTION',    '觀望',     'sv-caution'
    else:             sig, sig_cn, sig_cls = 'AVOID',      '迴避',     'sv-avoid'

    strengths, weaknesses = [], []
    if s['momentum'] >= 3:  strengths.append('技術趨勢強勁，均線多頭排列，適合趁拉回進場')
    if s['dividend'] >= 3:  strengths.append(f'殖利率 {div_yield:.1f}%，配息豐厚，適合長期存股')
    if s['cost']     >= 3:  strengths.append(f'費用率 {expense_ratio:.2f}%，成本低廉，長期複利效果佳')
    if s['scale']    >= 3:  strengths.append(f'規模 {total_assets:.0f} 億元，流動性充足，買賣彈性高')
    if three_yr > 10:       strengths.append(f'3年年化報酬 {three_yr:.1f}%，長期績效優異')
    if rsi_v < 40:          strengths.append(f'RSI {rsi_v:.0f} 低檔，技術面偏低，分批布局機會')

    if s['momentum'] <= 1:  weaknesses.append('短期趨勢偏弱，建議等待均線翻多再布局，避免追高')
    if s['dividend'] <= 1 and div_yield > 0:  weaknesses.append(f'殖利率 {div_yield:.1f}% 偏低，作為存股工具吸引力有限')
    if s['cost']     <= 1 and expense_ratio > 0: weaknesses.append(f'費用率 {expense_ratio:.2f}% 偏高，長期拖累報酬不可忽視')
    if s['scale']    <= 1:  weaknesses.append('規模較小，流動性風險較高，注意買賣價差')
    if rsi_v > 70:          weaknesses.append(f'RSI {rsi_v:.0f} 超買，短線追高需謹慎，等待拉回再進場')

    return {
        'signal':    sig,
        'signalCn':  sig_cn,
        'signalCls': sig_cls,
        'score':     round(pct * 100),
        'grades': {
            'momentum': grade(s['momentum']),
            'dividend': grade(s['dividend']),
            'cost':     grade(s['cost']),
            'scale':    grade(s['scale']),
        },
        'strengths':  strengths[:3],
        'weaknesses': weaknesses[:2],
        'isEtfScore': True,
    }


def gen_risks(price, ma20, rsi, vol_ratio, week52h,
              pe=0, fwd_pe=0, beta=1.0, debt_equity=0,
              sector='', industry=''):
    risks = []
    from_high = (price - week52h) / week52h * 100 if week52h > 0 else 0
    ref_pe = fwd_pe if fwd_pe > 0 else pe

    # Valuation risk
    if ref_pe > 60:
        risks.append({'level':'high',   'category':'估值風險', 'text':f'本益比 {ref_pe:.0f}x 極高，一旦業績不如預期將面臨大幅估值修正，建議分批布局'})
    elif ref_pe > 35:
        risks.append({'level':'medium', 'category':'估值風險', 'text':f'本益比 {ref_pe:.0f}x 偏高，成長需持續兌現以支撐目前股價'})

    # Technical risk
    if rsi > 75:
        risks.append({'level':'high',   'category':'技術風險', 'text':f'RSI {rsi:.0f} 嚴重超買，技術面高度過熱，短線回調風險極高'})
    elif rsi > 70:
        risks.append({'level':'medium', 'category':'技術風險', 'text':f'RSI {rsi:.0f} 進入超買區，短線追高需謹慎，建議等待拉回'})

    if from_high > -5:
        risks.append({'level':'medium', 'category':'技術風險', 'text':f'股價距52週高點僅 {abs(from_high):.1f}%，面臨歷史強壓力區，突破需大量確認'})

    # Trend risk
    if price < ma20:
        risks.append({'level':'high',   'category':'趨勢風險', 'text':'股價跌破 MA20 均線，中期趨勢可能轉弱，建議降低部位等待均線翻多'})

    # Market risk
    if beta > 1.5:
        risks.append({'level':'medium', 'category':'市場風險', 'text':f'Beta {beta:.1f}，波動性高於大盤 {(beta-1)*100:.0f}%，市場修正時跌幅將顯著放大'})

    # Financial risk
    if debt_equity > 200:
        risks.append({'level':'high',   'category':'財務風險', 'text':f'負債股東權益比 {debt_equity:.0f}%，財務槓桿偏高，升息或景氣下行壓力大'})
    elif debt_equity > 100:
        risks.append({'level':'medium', 'category':'財務風險', 'text':f'負債比 {debt_equity:.0f}%，需關注現金流與利息覆蓋能力'})

    # Chip / volume risk
    if vol_ratio > 3.5:
        risks.append({'level':'medium', 'category':'籌碼風險', 'text':f'成交量爆量（{vol_ratio:.1f}x 均量），短期獲利了結賣壓可能增加，注意籌碼鬆動'})

    # Macro risk — severity depends on valuation and sector
    is_high_pe = ref_pe > 30 or ref_pe == 0  # unknown PE treated as growth
    macro_level = 'medium' if (is_high_pe or 'Technology' in sector) else 'low'
    risks.append({'level': macro_level, 'category': '總經風險',
                  'text': '聯準會利率政策仍具不確定性，高本益比成長股對利率敏感度高' if is_high_pe
                          else '宏觀經濟與通膨走勢仍需追蹤，景氣下行時需評估盈利韌性'})

    # Geopolitical risk — only meaningful for tech/semiconductor exposed to China trade
    is_supply_chain = 'Semiconductor' in industry or 'Electronic' in industry or 'Technology' in sector
    if is_supply_chain:
        risks.append({'level': 'low', 'category': '地緣風險',
                      'text': '中美科技競爭持續，出口管制政策可能影響供應鏈佈局與市場准入'})
    else:
        risks.append({'level': 'low', 'category': '業務風險',
                      'text': '市場競爭加劇與技術迭代加速，財報不如預期或展望保守將引發短期波動'})

    return risks[:6]

def gen_strategy(price, ma5, ma20, ma60, rsi, levels, info=None):
    stop = max(levels['support1'] * 0.97, price * 0.90)

    # Leveraged / inverse ETFs need completely different strategy language
    _info     = info or {}
    _sym      = _info.get('symbol', '').upper().replace('.TW','').replace('.TWO','')
    _name     = (_info.get('longName','') + _info.get('shortName','')).upper()
    _is_lev   = _sym.endswith('L') or '槓桿' in _name or '2倍' in _name
    _is_inv   = _sym.endswith('R') or _sym.endswith('B') or '反向' in _name

    if _is_lev:
        long_t  = '不適合長期持有，槓桿耗損效應將侵蝕長期報酬，建議操作週期以日至週為限'
        swing_t = f'趨勢明確時可短線追進，突破 ${levels["resistance1"]:.2f} 加碼，嚴格設 MA20 止損'
        short_t = f'短線支撐參考 ${levels["support1"]:.2f}，重倉風險極高，部位控制在總資金 10% 內'
    elif _is_inv:
        long_t  = '不適合長期持有，僅限短線空頭避險，持有超過 2 週複利效應將大幅偏離 -1 倍報酬'
        swing_t = f'看空市場時可短線介入，指數反彈（本ETF回落至 ${levels["support1"]:.2f}）時注意止損'
        short_t = f'操作週期建議 1-5 個交易日，平倉後勿持有過夜部位過重'
    elif price > ma20 and rsi < 70:
        long_t  = f'逢回布局，回測 MA20（${ma20:.2f}）附近加倉，止損設 MA60（${ma60:.2f}）下方 3%'
        swing_t = f'波段操作：突破近期高點 ${levels["resistance1"]:.2f} 後加碼，回踩 MA20 止損'
        short_t = f'短線留意支撐位 ${levels["support1"]:.2f} 附近反彈機會，嚴格設止損'
    else:
        long_t  = f'等待股價站穩 MA60（${ma60:.2f}）後再布局，降低進場風險'
        swing_t = f'等待回測 MA20（${ma20:.2f}）確認支撐後入場，止損設前低'
        short_t = f'技術面偏弱，觀望為主，等待均線金叉信號再行動'

    # Price targets: prefer analyst data, fall back to technical levels
    t_mean = safe_float((info or {}).get('targetMeanPrice', 0))
    t_high = safe_float((info or {}).get('targetHighPrice', 0))
    t_low  = safe_float((info or {}).get('targetLowPrice',  0))

    if t_mean > price:
        bull_t    = round(t_high if t_high > t_mean else t_mean * 1.08, 1)
        neutral_t = round(t_mean, 1)
        bear_t    = round(t_low  if 0 < t_low < price else price * 0.90, 1)
    else:
        # No valid analyst coverage — derive from technical levels
        bull_t    = round(max(levels['resistance2'], price * 1.15), 1)
        neutral_t = round(levels['resistance1'] if levels['resistance1'] > price else price * 1.08, 1)
        bear_t    = round(levels['support2']    if levels['support2']    < price else price * 0.90, 1)

    return {
        'long': long_t, 'swing': swing_t, 'short': short_t,
        'stopLoss':       round(stop, 2),
        'bullTarget':     bull_t,
        'neutralTarget':  neutral_t,
        'bearTarget':     bear_t,
    }

# ── Routes ────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/market')
def get_market():
    cached = _cache_get('market')
    if cached: return jsonify(cached)
    syms = {
        'vix':    '^VIX',
        'sp500':  '^GSPC',
        'nasdaq': '^IXIC',
        'dow':    '^DJI',
        'gold':   'GC=F',
        'dxy':    'DX-Y.NYB',
    }
    result = {}
    for key, sym in syms.items():
        try:
            h = yf.Ticker(sym).history(period='2d')
            if len(h) >= 2:
                cur  = safe_float(h['Close'].iloc[-1])
                prev = safe_float(h['Close'].iloc[-2])
                pct  = (cur / prev - 1) * 100 if prev else 0
                result[key] = {'v': round(cur, 2), 'pct': round(pct, 2)}
            elif len(h) == 1:
                result[key] = {'v': round(safe_float(h['Close'].iloc[-1]), 2), 'pct': 0}
            else:
                result[key] = None
        except Exception:
            result[key] = None

    vix_val = (result.get('vix') or {}).get('v', 20)
    if   vix_val < 15: label, cls = '極度貪婪', 'greed-hi'
    elif vix_val < 20: label, cls = '貪婪',     'greed'
    elif vix_val < 25: label, cls = '中性',     'neutral-m'
    elif vix_val < 30: label, cls = '恐懼',     'fear'
    else:              label, cls = '極度恐懼', 'fear-hi'
    result['vixLabel'] = label
    result['vixCls']   = cls
    _cache_set('market', result, ttl=60)
    return jsonify(result)


@app.route('/api/fundamentals/<ticker>')
def get_fundamentals(ticker):
    ticker = ticker.upper().strip()
    cached = _cache_get(f'fund:{ticker}')
    if cached: return jsonify(cached)
    try:
        stock  = yf.Ticker(ticker)
        info   = stock.info

        # ── Cash Flow ──
        ocf_val = fcf_val = 0
        try:
            cf = stock.cashflow
            if cf is not None and not cf.empty:
                for lbl in ['Operating Cash Flow', 'Total Cash From Operating Activities']:
                    if lbl in cf.index:
                        ocf_val = safe_float(cf.loc[lbl].iloc[0]); break
                for lbl in ['Free Cash Flow']:
                    if lbl in cf.index:
                        fcf_val = safe_float(cf.loc[lbl].iloc[0]); break
                if fcf_val == 0 and ocf_val != 0:
                    for lbl in ['Capital Expenditure', 'Capital Expenditures']:
                        if lbl in cf.index:
                            fcf_val = ocf_val + safe_float(cf.loc[lbl].iloc[0]); break
        except Exception:
            pass

        # ── Institutional holders ──
        top_holders = []
        try:
            ih = stock.institutional_holders
            if ih is not None and not ih.empty:
                cols = [str(c) for c in ih.columns]
                # Find the holder name column (non-numeric, non-date column)
                name_col = next((c for c in cols if 'holder' in c.lower() or 'institution' in c.lower()), None)
                pct_col  = next((c for c in cols if 'pct' in c.lower() or '%' in c or 'out' in c.lower()), None)
                val_col  = next((c for c in cols if 'value' in c.lower()), None)
                if name_col:
                    for _, row in ih.head(5).iterrows():
                        holder = str(row[name_col])
                        if holder and holder != 'nan' and not holder[:4].isdigit():
                            pct = safe_float(row[pct_col]) if pct_col else 0
                            val = safe_float(row[val_col]) if val_col else 0
                            pct_disp = round(pct * 100, 2) if pct < 1 else round(pct, 2)
                            top_holders.append({
                                'holder': holder[:35],
                                'pct':    pct_disp,
                                'value':  round(val / 1e9, 2),
                            })
        except Exception:
            pass

        # ── Earnings date ──
        earnings_date = None
        try:
            cal = stock.calendar
            if cal is not None and not cal.empty:
                col = cal.columns[0]
                earnings_date = str(col.date()) if hasattr(col, 'date') else str(col)[:10]
        except Exception:
            pass

        mktcap = safe_float(info.get('marketCap', 0))
        fcf_yield = round(fcf_val / mktcap * 100, 2) if mktcap and fcf_val else 0
        price = safe_float(info.get('currentPrice', info.get('regularMarketPrice', 0)))
        pfcf = round(mktcap / fcf_val, 1) if fcf_val and fcf_val > 0 else None

        result = {
            'ticker':       ticker,
            'ocf':          round(ocf_val / 1e9, 2),
            'fcf':          round(fcf_val / 1e9, 2),
            'fcfYield':     fcf_yield,
            'pfcf':         pfcf,
            'debtEquity':   round(safe_float(info.get('debtToEquity', 0)), 1),
            'currentRatio': round(safe_float(info.get('currentRatio', 0)), 2),
            'roe':          round(safe_float(info.get('returnOnEquity', 0)) * 100, 1),
            'roa':          round(safe_float(info.get('returnOnAssets', 0)) * 100, 1),
            'profitMargin': round(safe_float(info.get('profitMargins', 0)) * 100, 1),
            'grossMargin':  round(safe_float(info.get('grossMargins', 0)) * 100, 1),
            'instPct':      round(safe_float(info.get('heldPercentInstitutions', 0)) * 100, 1),
            'insiderPct':   round(safe_float(info.get('heldPercentInsiders', 0)) * 100, 1),
            'shortRatio':   round(safe_float(info.get('shortRatio', 0)), 1),
            'shortPct':     round(safe_float(info.get('shortPercentOfFloat', 0)) * 100, 2),
            'earningsDate': earnings_date,
            'epsEst':       round(safe_float(info.get('forwardEps', 0)), 2),
            'revGrowth':    round(safe_float(info.get('revenueGrowth', 0)) * 100, 1),
            'epsGrowth':    round(safe_float(info.get('earningsGrowth', 0)) * 100, 1),
            'topHolders':   top_holders,
        }
        _cache_set(f'fund:{ticker}', result)
        return jsonify(result)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/stock/<ticker>')
def get_stock(ticker):
    ticker = ticker.upper().strip()
    cached = _cache_get(f'stock:{ticker}')
    if cached: return jsonify(cached)
    try:
        stock  = yf.Ticker(ticker)
        info   = stock.info
        hist   = stock.history(period='1y')

        if hist.empty:
            return jsonify({'error': f'找不到股票 {ticker}，請確認代碼是否正確'}), 404

        # ── Indicators ──
        hist['MA5']  = hist['Close'].rolling(5).mean()
        hist['MA20'] = hist['Close'].rolling(20).mean()
        hist['MA60'] = hist['Close'].rolling(60).mean()

        macd_s, sig_s, hist_s = calc_macd(hist['Close'])
        hist['MACD']     = macd_s
        hist['Signal']   = sig_s
        hist['MACDHist'] = hist_s
        hist['RSI']      = calc_rsi(hist['Close'])

        bb_upper, bb_mid, bb_lower = calc_bollinger(hist['Close'])
        hist['BB_upper'] = bb_upper
        hist['BB_mid']   = bb_mid
        hist['BB_lower'] = bb_lower

        # ── Core values ──
        price = last_valid(hist['Close'])
        prev  = safe_float(hist['Close'].dropna().iloc[-2]) if len(hist['Close'].dropna()) > 1 else price
        change     = price - prev
        change_pct = change / prev * 100 if prev else 0

        ma5    = last_valid(hist['MA5'])
        ma20   = last_valid(hist['MA20'])
        ma60   = last_valid(hist['MA60'])
        macd_v = last_valid(hist['MACD'])
        dea_v  = last_valid(hist['Signal'])
        macd_h = last_valid(hist['MACDHist'])
        rsi_v  = last_valid(hist['RSI'])

        avg_vol   = safe_float(hist['Volume'].rolling(20).mean().iloc[-1])
        curr_vol  = last_valid(hist['Volume'])
        vol_ratio = curr_vol / avg_vol if avg_vol > 0 else 1.0

        week52h = safe_float(info.get('fiftyTwoWeekHigh', hist['High'].max()))
        week52l = safe_float(info.get('fiftyTwoWeekLow',  hist['Low'].min()))

        bb_u = last_valid(hist['BB_upper'])
        bb_m = last_valid(hist['BB_mid'])
        bb_l = last_valid(hist['BB_lower'])
        bb_width = round((bb_u - bb_l) / bb_m * 100, 2) if bb_m else 0
        bb_pos   = round((price - bb_l) / (bb_u - bb_l) * 100, 1) if (bb_u - bb_l) else 50

        # ── Quick financials from info (fast) ──
        short_ratio   = round(safe_float(info.get('shortRatio', 0)), 1)
        short_pct     = round(safe_float(info.get('shortPercentOfFloat', 0)) * 100, 2)
        profit_margin = round(safe_float(info.get('profitMargins', 0)) * 100, 1)
        roe           = round(safe_float(info.get('returnOnEquity', 0)) * 100, 1)
        gross_margin  = round(safe_float(info.get('grossMargins', 0)) * 100, 1)
        debt_equity   = round(safe_float(info.get('debtToEquity', 0)), 1)
        inst_pct      = round(safe_float(info.get('heldPercentInstitutions', 0)) * 100, 1)
        insider_pct   = round(safe_float(info.get('heldPercentInsiders', 0)) * 100, 1)
        rev_growth    = round(safe_float(info.get('revenueGrowth', 0)) * 100, 1)
        eps_growth    = round(safe_float(info.get('earningsGrowth', 0)) * 100, 1)
        fwd_eps       = round(safe_float(info.get('forwardEps', 0)), 2)

        levels      = get_levels(hist)
        conclusions = gen_conclusions(price, ma5, ma20, ma60, macd_v, dea_v, rsi_v, vol_ratio)
        catalysts   = gen_catalysts(price, ma5, ma20, ma60, macd_v, dea_v, rsi_v, vol_ratio, week52h, info)
        risks       = gen_risks(price, ma20, rsi_v, vol_ratio, week52h,
                                pe=safe_float(info.get('trailingPE',0)),
                                fwd_pe=safe_float(info.get('forwardPE',0)),
                                beta=safe_float(info.get('beta',1)),
                                debt_equity=safe_float(info.get('debtToEquity',0)),
                                sector=info.get('sector',''),
                                industry=info.get('industry',''))
        strategy    = gen_strategy(price, ma5, ma20, ma60, rsi_v, levels, info=info)
        returns     = calc_returns(hist)
        invest_val  = gen_investment_value(
            price, ma5, ma20, ma60, macd_v, dea_v, rsi_v,
            pe=safe_float(info.get('trailingPE',0)),
            fwd_pe=safe_float(info.get('forwardPE',0)),
            roe=roe, profit_margin=profit_margin,
            rev_growth=rev_growth, eps_growth=eps_growth,
            beta=safe_float(info.get('beta',1)),
            debt_equity=debt_equity, vol_ratio=vol_ratio)

        # ── Quarterly revenue ──
        quarterly = []
        try:
            qf = stock.quarterly_financials
            if not qf.empty:
                for label in ['Total Revenue', 'Revenue']:
                    if label in qf.index:
                        row = qf.loc[label]
                        for col in row.index[:5]:
                            v = safe_float(row[col])
                            if v > 0:
                                quarterly.append({'period': str(col)[:7], 'revenue': round(v / 1e6, 1)})
                        break
        except Exception:
            pass

        def clean(lst):
            res = []
            for x in lst:
                try:
                    f = float(x)
                    res.append(None if (np.isnan(f) or np.isinf(f)) else round(f, 4))
                except Exception:
                    res.append(None)
            return res

        dates = hist.index.strftime('%Y-%m-%d').tolist()

        result = {
            'ticker':       ticker,
            'name':         tw_cn_name(ticker, info.get('longName', info.get('shortName', ticker))),
            'sector':       info.get('sector', ''),
            'industry':     info.get('industry', ''),
            'country':      info.get('country', ''),
            'description':  (info.get('longBusinessSummary', '') or '')[:300],
            'price':        round(price, 3),
            'change':       round(change, 3),
            'changePct':    round(change_pct, 2),
            'open':         round(last_valid(hist['Open']), 2),
            'high':         round(last_valid(hist['High']), 2),
            'low':          round(last_valid(hist['Low']), 2),
            'prevClose':    round(prev, 2),
            'volume':       safe_int(curr_vol),
            'avgVolume':    safe_int(avg_vol),
            'volRatio':     round(vol_ratio, 2),
            'marketCap':    safe_float(info.get('marketCap', 0)),
            'pe':           round(safe_float(info.get('trailingPE', 0)), 2),
            'forwardPe':    round(safe_float(info.get('forwardPE', 0)), 2),
            'eps':          round(safe_float(info.get('trailingEps', 0)), 2),
            'fwdEps':       fwd_eps,
            'beta':         round(safe_float(info.get('beta', 0)), 2),
            'divYield':     round(safe_float(info.get('dividendYield', 0)), 2),
            'sharesOut':    safe_int(info.get('sharesOutstanding', 0)),
            'week52High':   round(week52h, 2),
            'week52Low':    round(week52l, 2),
            'analystTarget': round(safe_float(info.get('targetMeanPrice', 0)), 2),
            'analystHigh':   round(safe_float(info.get('targetHighPrice', 0)), 2),
            'analystLow':    round(safe_float(info.get('targetLowPrice', 0)), 2),
            'recMean':       round(safe_float(info.get('recommendationMean', 3)), 2),
            'numAnalysts':   safe_int(info.get('numberOfAnalystOpinions', 0)),
            'shortRatio':   short_ratio,
            'shortPct':     short_pct,
            'profitMargin': profit_margin,
            'grossMargin':  gross_margin,
            'roe':          roe,
            'debtEquity':   debt_equity,
            'instPct':      inst_pct,
            'insiderPct':   insider_pct,
            'revGrowth':    rev_growth,
            'epsGrowth':    eps_growth,
            'ma5':    round(ma5, 2),
            'ma20':   round(ma20, 2),
            'ma60':   round(ma60, 2),
            'macdVal':  round(macd_v, 2),
            'deaVal':   round(dea_v, 2),
            'macdHist': round(macd_h, 2),
            'rsi':      round(rsi_v, 2),
            'bbUpper':  round(bb_u, 2),
            'bbMid':    round(bb_m, 2),
            'bbLower':  round(bb_l, 2),
            'bbWidth':  bb_width,
            'bbPos':    bb_pos,
            'levels':      levels,
            'conclusions': conclusions,
            'catalysts':   catalysts,
            'risks':       risks,
            'strategy':    strategy,
            'returns':     returns,
            'investValue': invest_val,
            'quarterly':   quarterly,
            'dates': dates,
            'ohlcv': {
                'open':   clean(hist['Open'].tolist()),
                'high':   clean(hist['High'].tolist()),
                'low':    clean(hist['Low'].tolist()),
                'close':  clean(hist['Close'].tolist()),
                'volume': [safe_int(x) for x in hist['Volume'].tolist()],
            },
            'ma': {
                'ma5':  clean(hist['MA5'].tolist()),
                'ma20': clean(hist['MA20'].tolist()),
                'ma60': clean(hist['MA60'].tolist()),
            },
            'macd': {
                'dif':  clean(hist['MACD'].tolist()),
                'dea':  clean(hist['Signal'].tolist()),
                'hist': clean(hist['MACDHist'].tolist()),
            },
            'bollinger': {
                'upper': clean(hist['BB_upper'].tolist()),
                'mid':   clean(hist['BB_mid'].tolist()),
                'lower': clean(hist['BB_lower'].tolist()),
            },
            'rsiSeries': clean(hist['RSI'].tolist()),
        }
        _cache_set(f'stock:{ticker}', result)
        return jsonify(result)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/news/<ticker>')
def get_news(ticker):
    ticker = ticker.upper().strip()
    cached = _cache_get(f'news:{ticker}')
    if cached: return jsonify(cached)
    try:
        stock     = yf.Ticker(ticker)
        raw       = stock.news or []
        articles  = []
        for item in raw[:12]:
            c         = item.get('content', {})
            title     = c.get('title', '')
            publisher = (c.get('provider') or {}).get('displayName', '')
            url       = (c.get('canonicalUrl') or {}).get('url', '')
            summary   = c.get('summary', '') or ''
            pub_time  = c.get('pubDate', '')
            if title:
                articles.append({
                    'title':     title,
                    'publisher': publisher,
                    'url':       url,
                    'summary':   summary[:180],
                    'pubTime':   pub_time,
                })
        result = {'ticker': ticker, 'articles': articles}
        _cache_set(f'news:{ticker}', result, ttl=180)
        return jsonify(result)
    except Exception as e:
        return jsonify({'ticker': ticker, 'articles': [], 'error': str(e)})


# ── Taiwan Helpers ────────────────────────────────────────────────────
def tw_normalize(raw):
    raw = raw.strip().upper()
    if raw.endswith('.TW') or raw.endswith('.TWO'):
        return raw
    return raw + '.TW'

def tw_display(ticker):
    return ticker.replace('.TWO', '').replace('.TW', '')

def gen_tw_risks(price, ma20, rsi, vol_ratio, week52h,
                 pe=0, fwd_pe=0, beta=1.0, debt_equity=0,
                 is_etf=False, inst_pct=0, ticker='', etf_name=''):
    risks = []
    from_high = (price - week52h) / week52h * 100 if week52h > 0 else 0
    ref_pe = fwd_pe if fwd_pe > 0 else pe

    if not is_etf:  # PE is not a meaningful metric for ETFs
        if ref_pe > 30:
            risks.append({'level':'high',   'category':'估值風險', 'text':f'本益比 {ref_pe:.0f}x 高於台股歷史均值（約15-20x），業績須持續超預期才能支撐估值'})
        elif ref_pe > 20:
            risks.append({'level':'medium', 'category':'估值風險', 'text':f'本益比 {ref_pe:.0f}x 略高，需關注業績成長是否持續兌現'})

    if rsi > 75:
        risks.append({'level':'high',   'category':'技術風險', 'text':f'RSI {rsi:.0f} 嚴重超買，技術面過熱，短線回調風險高，建議等待拉回再布局'})
    elif rsi > 70:
        risks.append({'level':'medium', 'category':'技術風險', 'text':f'RSI {rsi:.0f} 進入超買區，短線追高需謹慎，可等待拉回均線再進場'})

    if from_high > -5:
        risks.append({'level':'medium', 'category':'技術風險', 'text':f'接近52週高點（距頂 {abs(from_high):.1f}%），面臨歷史強壓力區，突破需大量配合'})

    if price < ma20:
        risks.append({'level':'high',   'category':'趨勢風險', 'text':'跌破 MA20 均線，中期趨勢可能轉弱，建議降低部位等待均線翻多'})

    if beta > 1.5:
        risks.append({'level':'medium', 'category':'市場風險', 'text':f'Beta {beta:.1f}，波動性顯著高於大盤，市場修正時跌幅將放大'})

    if debt_equity > 150:
        risks.append({'level':'high',   'category':'財務風險', 'text':f'負債股東權益比 {debt_equity:.0f}%，財務槓桿偏高，利率上升或景氣下行壓力大'})
    elif debt_equity > 80:
        risks.append({'level':'medium', 'category':'財務風險', 'text':f'負債比 {debt_equity:.0f}%，需關注現金流與利息覆蓋能力'})

    if vol_ratio > 3.5:
        risks.append({'level':'medium', 'category':'籌碼風險', 'text':f'成交量爆量（{vol_ratio:.1f}x 均量），短期獲利了結賣壓可能增加，注意籌碼鬆動'})

    # ETF-specific risk: differentiate leveraged/inverse from regular
    if is_etf:
        sym  = ticker.upper().replace('.TW', '').replace('.TWO', '')
        name_upper = etf_name.upper()
        is_lev = sym.endswith('L') or '槓桿' in name_upper or '2倍' in name_upper
        is_inv = sym.endswith('R') or sym.endswith('B') or '反向' in name_upper or 'INVERSE' in name_upper
        if is_lev:
            risks.append({'level':'high', 'category':'槓桿耗損風險',
                          'text':'槓桿ETF每日重新平衡，長期持有因複利衰減效應（beta slippage）報酬將顯著偏離2倍指數，不適合長期持有或定期定額'})
        elif is_inv:
            risks.append({'level':'high', 'category':'方向風險',
                          'text':'反向ETF僅適合短線避險，長期持有因複利效應將顯著偏離預期報酬，須嚴格設定停利停損'})
        else:
            risks.append({'level':'low', 'category':'追蹤風險',
                          'text':'ETF追蹤誤差與折溢價可能影響實際報酬，建議定期確認 NAV 與市價差異'})

    # Geopolitical risk — always relevant for Taiwan
    risks.append({'level':'medium', 'category':'地緣風險',
                  'text':'兩岸關係緊張及地緣政治局勢仍是台股最大不確定因素，可能引發外資快速撤離並衝擊市場'})

    # Macro risk — always relevant for Taiwan
    risks.append({'level':'medium', 'category':'總經風險',
                  'text':'台灣央行利率政策、新台幣匯率走勢及全球景氣循環均對台股形成壓力，需密切追蹤'})

    # Foreign ownership risk — only if inst_pct is actually high
    if inst_pct >= 25:
        risks.append({'level':'low', 'category':'外資風險',
                      'text':f'外資持股 {inst_pct:.0f}%，全球風險趨避情緒升溫時可能引發大量賣超，衝擊市場流動性'})

    return risks[:6]

def gen_tw_catalysts(price, ma5, ma20, ma60, macd, dea, rsi,
                     vol_ratio, week52h, info, is_etf=False):
    cats = []
    if price >= week52h * 0.97:
        cats.append({'num': 1, 'text': '突破或接近52週高點，強勢創高訊號',
                     'sub': '價格創新高，市場認可度提升，突破確認後動能強勁'})
    if macd > dea and macd > 0:
        cats.append({'num': len(cats)+1, 'text': 'MACD 金叉且在零軸上方，多頭動能強勁',
                     'sub': '短中期均偏多，技術面轉強訊號確立'})
    if vol_ratio >= 1.5:
        cats.append({'num': len(cats)+1, 'text': f'成交量放大（{vol_ratio:.1f}x 均量），法人積極介入',
                     'sub': '三大法人買超，籌碼結構改善，主力護盤意願強'})
    if price > ma5 > ma20 > ma60:
        cats.append({'num': len(cats)+1, 'text': '均線多頭排列完整，趨勢強勢',
                     'sub': '短中長期均線支撐，回撐布局機會，趨勢延續性高'})

    if is_etf:
        name      = (info.get('longName', '') + ' ' + info.get('shortName', '')).upper()
        # yfinance returns dividendYield as a percentage for TW tickers (e.g. 6.65 = 6.65%)
        div_yield = safe_float(info.get('dividendYield', 0))
        sym       = info.get('symbol', '').upper().replace('.TW', '').replace('.TWO', '')

        is_leveraged = sym.endswith('L') or '槓桿' in name or '2倍' in name
        is_inverse   = sym.endswith('R') or '反向' in name or 'INVERSE' in name
        is_bond      = '債' in name or 'BOND' in name
        is_esg       = 'ESG' in name or '永續' in name
        is_income    = '高息' in name or '高股息' in name or div_yield >= 4

        if is_leveraged:
            item0 = {'text': '短線波段放大工具，掌握指數趨勢倍數報酬',
                     'sub':  '適合有操作經驗的短線投資人，不適合長期持有或定期定額'}
        elif is_inverse:
            item0 = {'text': '空頭避險工具，指數下跌時反向獲利',
                     'sub':  '適合短線避險或看空操作，不適合長期持有'}
        elif is_bond:
            item0 = {'text': '固定收益特性，股債配置降低整體波動',
                     'sub':  '與股票低相關性，有效分散組合風險，適合穩健型投資人'}
        elif is_esg:
            item0 = {'text': 'ESG永續趨勢，國際機構資金優先配置標的',
                     'sub':  '符合全球ESG投資潮流，機構法人偏好，長期估值支撐佳'}
        else:
            item0 = {'text': '長期定期定額最佳工具，分散風險效果佳',
                     'sub':  '追蹤指數，分散個股風險，適合長期穩健投資人'}

        item1 = {'text': '費用率低廉，長期複利效果顯著優越',
                 'sub':  '相較主動基金費用低，長期績效差異大'}

        if is_leveraged or is_inverse:
            item2 = {'text': '短線操作為主，嚴格控制持有時間與部位',
                     'sub':  '複利衰減效應使長期持有報酬大幅偏離預期，建議持有週期不超過數週'}
        elif div_yield == 0:
            item2 = {'text': '不配息設計，股息自動滾入淨值複利效果佳',
                     'sub':  '股利完整保留於淨值，免配息扣稅，長期資本累積效率更高'}
        elif is_income:
            item2 = {'text': f'高殖利率 {div_yield:.1f}%，現金流穩定豐厚',
                     'sub':  '高額定期配息，適合退休規劃與追求現金流的存股族'}
        else:
            item2 = {'text': f'配息 {div_yield:.1f}%，適合退休規劃與現金流需求',
                     'sub':  '定期配息提供穩定現金流，適合保守型投資人'}

        item3 = {'text': '流動性佳，買賣彈性高於一般基金',
                 'sub':  '交易所掛牌，隨時買賣，不受申購贖回限制'}

        extras = [item0, item1, item2, item3]
    else:
        sector     = info.get('sector', '')
        industry   = info.get('industry', '')
        div_yield  = safe_float(info.get('dividendYield', 0))   # TW: already %
        inst_pct   = round(safe_float(info.get('heldPercentInstitutions', 0)) * 100, 1)
        rev_growth = round(safe_float(info.get('revenueGrowth', 0)) * 100, 1)
        target     = safe_float(info.get('targetMeanPrice', 0))

        extras = []

        # Analyst target — only if meaningful upside
        if target > price * 1.1:
            upside = (target / price - 1) * 100
            extras.append({'text': f'分析師共識目標 ${target:.1f}（潛在漲幅 +{upside:.0f}%）',
                           'sub':  '券商看好後市，平均目標價相對現價仍有顯著上漲空間'})

        # Sector — neutral, fact-based (no macro timing claims)
        if 'Semiconductor' in industry or 'Electronic' in industry or 'Technology' in sector:
            extras.append({'text': 'AI / 半導體供應鏈長期成長邏輯清晰',
                           'sub':  '受惠全球算力與終端裝置需求擴張，台廠在供應鏈中地位穩固'})
        elif 'Financial' in sector or 'Insurance' in industry or 'Bank' in industry:
            extras.append({'text': '金融業務多元化，利差與手續費收入組合穩定',
                           'sub':  '業務涵蓋零售銀行、壽險、財管等，收入結構分散'})
        elif 'Basic Materials' in sector or 'Chemical' in industry or 'Steel' in industry:
            extras.append({'text': '原材料產業具景氣循環特性，現金流相對充沛',
                           'sub':  '景氣回升期間受惠產品報價上漲，自由現金流改善'})
        elif 'Consumer' in sector:
            extras.append({'text': '品牌護城河穩固，定價能力強',
                           'sub':  '高品牌忠誠度保護利潤率，消費需求具韌性'})

        # Revenue growth — only when positive
        if rev_growth > 15:
            extras.append({'text': f'營收年增 {rev_growth:.0f}%，業績高速成長',
                           'sub':  '高速成長印證商業模式可行，機構法人持續上調目標價'})
        elif rev_growth > 5:
            extras.append({'text': f'營收成長 {rev_growth:.0f}%，基本面持續改善',
                           'sub':  '成長軌道持續，盈利品質穩定，本益比有基本面支撐'})
        elif rev_growth > 0:
            extras.append({'text': f'營收小幅成長 {rev_growth:.1f}%，業績逐步回穩',
                           'sub':  '成長動能初步回升，若下季加速將成更強力催化劑'})
        # rev_growth <= 0：不加，負成長不是催化劑

        # Institutional holding — only if meaningfully high
        if inst_pct >= 30:
            extras.append({'text': f'外資持股 {inst_pct:.0f}%，法人籌碼穩固',
                           'sub':  '機構長線佈局，籌碼結構穩定，護盤意願強'})

        # Dividend — only if actually paying a meaningful yield
        if div_yield >= 5:
            extras.append({'text': f'高殖利率 {div_yield:.1f}%，現金流豐厚',
                           'sub':  '高股息防禦特性，適合存股族，配息穩定提供抗跌保護'})
        elif div_yield >= 2:
            extras.append({'text': f'殖利率 {div_yield:.1f}%，股東回饋穩定',
                           'sub':  '定期現金股利，配息政策明確，適合長線持有'})

    for item in extras:
        if len(cats) >= 4:
            break
        cats.append({'num': len(cats) + 1, **item})

    # Fallback: factual items to avoid faking positives
    if len(cats) < 2:
        fwd_pe = safe_float(info.get('forwardPE', 0))
        pe     = safe_float(info.get('trailingPE', 0))
        beta   = safe_float(info.get('beta', 0))
        ref_pe = fwd_pe if fwd_pe > 0 else pe
        if ref_pe > 0:
            cats.append({'num': len(cats)+1,
                         'text': f'本益比 {ref_pe:.1f}x，評估當前估值合理性',
                         'sub':  '建議與同業及歷史均值比較，判斷是否仍有布局價值'})
        if beta > 0 and len(cats) < 2:
            cats.append({'num': len(cats)+1,
                         'text': f'Beta {beta:.2f}，{"波動低於大盤，適合穩健布局" if beta < 1 else "波動較高，適合積極型投資人"}',
                         'sub':  '了解個股波動性有助於設定適當部位與停損點'})

    return cats[:4]


# ── Taiwan Routes ─────────────────────────────────────────────────────
@app.route('/portfolio')
def portfolio():
    return render_template('portfolio.html')


PORTFOLIO_FILE = os.path.join(os.path.dirname(__file__), 'portfolio_data.json')
_portfolio_lock = threading.Lock()


@app.route('/api/portfolio/data', methods=['GET', 'POST'])
def portfolio_data_api():
    """投資組合資料的伺服器端持久化，讓持倉跨裝置同步（單一使用者）。"""
    if request.method == 'GET':
        try:
            with open(PORTFOLIO_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError, ValueError):
            data = {'groups': {}, 'holdings': []}
        return jsonify({
            'groups':   data.get('groups', {}) or {},
            'holdings': data.get('holdings', []) or [],
        })

    body = request.get_json(force=True) or {}
    data = {
        'groups':   body.get('groups', {}) or {},
        'holdings': body.get('holdings', []) or [],
    }
    with _portfolio_lock:
        with open(PORTFOLIO_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    return jsonify({'ok': True})


def _quote_for_ticker(code):
    """單檔即時報價 {price, changePct, name}；台股自動補 .TW/.TWO、美股直接用代碼，含快取與負向快取。"""
    code = (code or '').strip().upper()
    if not code:
        return {'price': None, 'changePct': None, 'name': ''}
    ck = f'pq:{code}'
    cached = _cache_get(ck)
    if cached:
        return cached
    is_tw   = bool(_re.match(r'^\d{4,6}[A-Z]?$', code))
    symbols = [code + '.TW', code + '.TWO'] if is_tw else [code]
    res = {'price': None, 'changePct': None, 'name': code}
    try:
        for sym in symbols:
            info  = yf.Ticker(sym).info
            price = safe_float(info.get('currentPrice', info.get('regularMarketPrice', 0)))
            if price > 0:
                prev = safe_float(info.get('previousClose', info.get('regularMarketPreviousClose', 0)))
                name = tw_cn_name(code, info.get('shortName', code)) if is_tw else info.get('shortName', code)
                res  = {'price': round(price, 2),
                        'changePct': round((price - prev) / prev * 100, 2) if prev else None,
                        'name': name}
                break
    except Exception:
        pass
    _cache_set(ck, res, ttl=30 if res['price'] else 600)
    return res


@app.route('/api/portfolio/priced')
def portfolio_priced_api():
    """一次回傳投資組合持倉＋即時報價（並行抓取），讓前端一次呼叫取代『清單＋逐檔報價』多次往返。"""
    holdings = _load_agent_cfg().get('holdings', [])
    codes    = [h.get('code', '') for h in holdings]
    qmap     = {}
    if codes:
        with ThreadPoolExecutor(max_workers=min(12, len(codes))) as ex:
            for code, q in zip(codes, ex.map(_quote_for_ticker, codes)):
                qmap[code] = q
    out = []
    for h in holdings:
        code = h.get('code', '')
        q    = qmap.get(code, {'price': None, 'changePct': None, 'name': code})
        out.append({
            'ticker':    code,
            'shares':    h.get('shares', 0),
            'costPrice': h.get('buy_price', 0),
            'buyDate':   h.get('date', ''),
            'note':      h.get('note', ''),
            'group':     h.get('group', ''),
            'price':     q['price'],
            'changePct': q['changePct'],
            'name':      q['name'],
        })
    return jsonify(out)


@app.route('/api/compare')
def compare_stocks():
    tickers_raw = request.args.get('tickers', '')
    tickers = [t.strip().upper() for t in tickers_raw.split(",") if t.strip()][:10]
    if not tickers:
        return jsonify([])

    def fetch_compare(ticker):
        is_tw = ticker.endswith('.TW') or ticker.endswith('.TWO')
        cache_key = f'cmp:{ticker}'
        cached = _cache_get(cache_key)
        if cached:
            return cached
        try:
            t = tw_normalize(ticker) if is_tw else ticker
            stock = yf.Ticker(t)
            info  = stock.info
            hist  = stock.history(period='6mo')
            if hist.empty:
                return {'ticker': ticker, 'error': '找不到資料'}

            close = hist['Close']
            price = last_valid(close)
            prev  = safe_float(close.dropna().iloc[-2]) if len(close.dropna()) > 1 else price
            change_pct = (price / prev - 1) * 100 if prev else 0

            n = len(close)
            ma20 = safe_float(close.rolling(min(20, n)).mean().iloc[-1])
            ma60 = safe_float(close.rolling(min(60, n)).mean().iloc[-1])
            rsi  = safe_float(calc_rsi(close).iloc[-1])
            macd_s, sig_s, hist_s = calc_macd(close)
            macd_v = safe_float(macd_s.iloc[-1])
            sig_v  = safe_float(sig_s.iloc[-1])

            week52h = safe_float(info.get('fiftyTwoWeekHigh', hist['High'].max()))
            week52l = safe_float(info.get('fiftyTwoWeekLow',  hist['Low'].min()))
            from52h = round((price / week52h - 1) * 100, 1) if week52h > 0 else 0

            avg_vol   = safe_float(hist['Volume'].rolling(min(20, n)).mean().iloc[-1])
            curr_vol  = last_valid(hist['Volume'])
            vol_ratio = curr_vol / avg_vol if avg_vol > 0 else 1.0

            bull = sum([price > ma20, price > ma60,
                        macd_v > sig_v, rsi < 50, vol_ratio > 1.3])
            if   bull >= 4: sig_label, sig_cls = '強勢多頭', 'sv-strong-buy'
            elif bull >= 3: sig_label, sig_cls = '偏多',     'sv-buy'
            elif bull >= 2: sig_label, sig_cls = '中性',     'sv-hold'
            else:           sig_label, sig_cls = '偏弱',     'sv-caution'

            div_yield = round(safe_float(info.get('dividendYield', 0)), 2)

            analyst_target = round(safe_float(info.get('targetMeanPrice', 0)), 2)
            upside = round((analyst_target / price - 1) * 100, 1) if analyst_target > 0 and price > 0 else 0

            result = {
                'ticker':       ticker,
                'name':         tw_cn_name(ticker, (info.get('shortName') or info.get('longName') or ticker)[:25]) if is_tw else (info.get('shortName') or info.get('longName') or ticker)[:25],
                'price':        round(price, 2),
                'changePct':    round(change_pct, 2),
                'pe':           round(safe_float(info.get('trailingPE',  0)), 1),
                'fwdPe':        round(safe_float(info.get('forwardPE',   0)), 1),
                'roe':          round(safe_float(info.get('returnOnEquity', 0)) * 100, 1),
                'divYield':     div_yield,
                'beta':         round(safe_float(info.get('beta', 0)), 2),
                'instPct':      round(safe_float(info.get('heldPercentInstitutions', 0)) * 100, 1),
                'revGrowth':    round(safe_float(info.get('revenueGrowth', 0)) * 100, 1),
                'profitMargin': round(safe_float(info.get('profitMargins', 0)) * 100, 1),
                'mktCap':       safe_float(info.get('marketCap', 0)),
                'rsi':          round(rsi, 1),
                'macdBull':     macd_v > sig_v,
                'volRatio':     round(vol_ratio, 2),
                'week52High':   round(week52h, 2),
                'week52Low':    round(week52l, 2),
                'from52High':   from52h,
                'analystTarget':analyst_target,
                'upside':       upside,
                'signal':       sig_label,
                'signalCls':    sig_cls,
                'aboveMa20':    price > ma20,
                'aboveMa60':    price > ma60,
                'isTw':         is_tw,
            }
            _cache_set(cache_key, result, ttl=180)
            return result
        except Exception as e:
            return {'ticker': ticker, 'error': str(e)[:80]}

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(fetch_compare, t): t for t in tickers}
        results = {}
        for f in as_completed(futures):
            r = f.result()
            results[r.get('ticker', '')] = r

    return jsonify([results.get(t, {'ticker': t, 'error': '載入失敗'}) for t in tickers])


@app.route('/tw')
def tw_index():
    return render_template('tw_stock.html')


@app.route('/api/tw/market')
def get_tw_market():
    cached = _cache_get('tw_market')
    if cached: return jsonify(cached)
    syms = {
        'twii':   '^TWII',
        'twoii':  '^TWOII',
        'usdtwd': 'USDTWD=X',
        'gold':   'GC=F',
        'vix':    '^VIX',
    }
    result = {}
    for key, sym in syms.items():
        try:
            h = yf.Ticker(sym).history(period='2d')
            if len(h) >= 2:
                cur  = safe_float(h['Close'].iloc[-1])
                prev = safe_float(h['Close'].iloc[-2])
                pct  = (cur / prev - 1) * 100 if prev else 0
                result[key] = {'v': round(cur, 2), 'pct': round(pct, 2)}
            elif len(h) == 1:
                result[key] = {'v': round(safe_float(h['Close'].iloc[-1]), 2), 'pct': 0}
            else:
                result[key] = None
        except Exception:
            result[key] = None

    vix_val = (result.get('vix') or {}).get('v', 20)
    if   vix_val < 15: label, cls = '極度貪婪', 'greed-hi'
    elif vix_val < 20: label, cls = '貪婪',     'greed'
    elif vix_val < 25: label, cls = '中性',     'neutral-m'
    elif vix_val < 30: label, cls = '恐懼',     'fear'
    else:              label, cls = '極度恐懼', 'fear-hi'
    result['vixLabel'] = label
    result['vixCls']   = cls
    _cache_set('tw_market', result, ttl=60)
    return jsonify(result)


@app.route('/api/tw/stock/<ticker>')
def get_tw_stock(ticker):
    ticker = tw_normalize(ticker)
    cached = _cache_get(f'tw_stock:{ticker}')
    if cached: return jsonify(cached)
    try:
        stock = yf.Ticker(ticker)
        info  = stock.info
        hist  = stock.history(period='1y')

        # Fallback .TWO
        if hist.empty and ticker.endswith('.TW'):
            alt   = ticker.replace('.TW', '.TWO')
            stock = yf.Ticker(alt)
            info  = stock.info
            hist  = stock.history(period='1y')
            if not hist.empty:
                ticker = alt

        if hist.empty:
            return jsonify({'error': f'找不到股票 {ticker}，請確認代碼是否正確'}), 404

        is_etf = info.get('quoteType', '').upper() == 'ETF'

        hist['MA5']  = hist['Close'].rolling(5).mean()
        hist['MA20'] = hist['Close'].rolling(20).mean()
        hist['MA60'] = hist['Close'].rolling(60).mean()
        macd_s, sig_s, hist_s = calc_macd(hist['Close'])
        hist['MACD']     = macd_s
        hist['Signal']   = sig_s
        hist['MACDHist'] = hist_s
        hist['RSI']      = calc_rsi(hist['Close'])
        bb_u, bb_m, bb_l = calc_bollinger(hist['Close'])
        hist['BB_upper'] = bb_u
        hist['BB_mid']   = bb_m
        hist['BB_lower'] = bb_l

        price = last_valid(hist['Close'])
        prev  = safe_float(hist['Close'].dropna().iloc[-2]) if len(hist['Close'].dropna()) > 1 else price
        change     = price - prev
        change_pct = change / prev * 100 if prev else 0

        ma5    = last_valid(hist['MA5'])
        ma20   = last_valid(hist['MA20'])
        ma60   = last_valid(hist['MA60'])
        macd_v = last_valid(hist['MACD'])
        dea_v  = last_valid(hist['Signal'])
        macd_h = last_valid(hist['MACDHist'])
        rsi_v  = last_valid(hist['RSI'])

        avg_vol  = safe_float(hist['Volume'].rolling(20).mean().iloc[-1])
        curr_vol = last_valid(hist['Volume'])
        vol_ratio = curr_vol / avg_vol if avg_vol > 0 else 1.0

        week52h = safe_float(info.get('fiftyTwoWeekHigh', hist['High'].max()))
        week52l = safe_float(info.get('fiftyTwoWeekLow',  hist['Low'].min()))

        bbu = last_valid(hist['BB_upper'])
        bbm = last_valid(hist['BB_mid'])
        bbl = last_valid(hist['BB_lower'])
        bb_width = round((bbu - bbl) / bbm * 100, 2) if bbm else 0
        bb_pos   = round((price - bbl) / (bbu - bbl) * 100, 1) if (bbu - bbl) else 50

        profit_margin = round(safe_float(info.get('profitMargins',     0)) * 100, 1)
        roe           = round(safe_float(info.get('returnOnEquity',     0)) * 100, 1)
        gross_margin  = round(safe_float(info.get('grossMargins',       0)) * 100, 1)
        debt_equity   = round(safe_float(info.get('debtToEquity',       0)), 1)
        inst_pct      = round(safe_float(info.get('heldPercentInstitutions', 0)) * 100, 1)
        insider_pct   = round(safe_float(info.get('heldPercentInsiders', 0)) * 100, 1)
        rev_growth    = round(safe_float(info.get('revenueGrowth',      0)) * 100, 1)
        eps_growth    = round(safe_float(info.get('earningsGrowth',     0)) * 100, 1)
        short_ratio   = round(safe_float(info.get('shortRatio',         0)), 1)
        short_pct     = round(safe_float(info.get('shortPercentOfFloat',0)) * 100, 2)
        fwd_eps       = round(safe_float(info.get('forwardEps',         0)), 2)

        levels      = get_levels(hist)
        conclusions = gen_conclusions(price, ma5, ma20, ma60, macd_v, dea_v, rsi_v, vol_ratio)
        catalysts   = gen_tw_catalysts(price, ma5, ma20, ma60, macd_v, dea_v, rsi_v,
                                       vol_ratio, week52h, info, is_etf)
        risks       = gen_tw_risks(price, ma20, rsi_v, vol_ratio, week52h,
                                   pe=safe_float(info.get('trailingPE', 0)),
                                   fwd_pe=safe_float(info.get('forwardPE', 0)),
                                   beta=safe_float(info.get('beta', 1)),
                                   debt_equity=safe_float(info.get('debtToEquity', 0)),
                                   is_etf=is_etf,
                                   inst_pct=inst_pct,
                                   ticker=ticker,
                                   etf_name=info.get('longName', '') + info.get('shortName', '') if is_etf else '')
        strategy    = gen_strategy(price, ma5, ma20, ma60, rsi_v, levels, info=info)
        returns     = calc_returns(hist)
        invest_val  = gen_investment_value(
            price, ma5, ma20, ma60, macd_v, dea_v, rsi_v,
            pe=safe_float(info.get('trailingPE', 0)),
            fwd_pe=safe_float(info.get('forwardPE', 0)),
            roe=roe, profit_margin=profit_margin,
            rev_growth=rev_growth, eps_growth=eps_growth,
            beta=safe_float(info.get('beta', 1)),
            debt_equity=debt_equity, vol_ratio=vol_ratio)

        quarterly = []
        try:
            qf = stock.quarterly_financials
            if not qf.empty:
                for lbl in ['Total Revenue', 'Revenue']:
                    if lbl in qf.index:
                        row = qf.loc[lbl]
                        for col in row.index[:5]:
                            v = safe_float(row[col])
                            if v > 0:
                                quarterly.append({'period': str(col)[:7], 'revenue': round(v / 1e6, 1)})
                        break
        except Exception:
            pass

        # ETF extra data
        etf_data = None
        if is_etf:
            ta = safe_float(info.get('totalAssets', 0))
            er = safe_float(info.get('annualReportExpenseRatio', info.get('totalExpenseRatio', 0)))
            if er > 1: er /= 100
            ta_yi = round(ta / 1e8, 1)
            er_pct = round(er * 100, 4) if er > 0 else 0
            three_yr = round(safe_float(info.get('threeYearAverageReturn', 0)) * 100, 2)
            five_yr  = round(safe_float(info.get('fiveYearAverageReturn',  0)) * 100, 2)
            ytd_ret  = round(safe_float(info.get('ytdReturn', 0)) * 100, 2)
            etf_data = {
                'totalAssets':   ta_yi,
                'expenseRatio':  er_pct,
                'threeYrReturn': three_yr,
                'fiveYrReturn':  five_yr,
                'ytdReturn':     ytd_ret,
                'category':      info.get('category', ''),
                'fundFamily':    info.get('fundFamily', ''),
            }
            invest_val = gen_etf_investment_value(
                price, ma5, ma20, ma60, macd_v, dea_v, rsi_v,
                div_yield=round(safe_float(info.get('dividendYield', 0)), 2),
                expense_ratio=er_pct,
                total_assets=ta_yi,
                vol_ratio=vol_ratio,
                ytd_return=ytd_ret,
                three_yr=three_yr)

        def clean(lst):
            res = []
            for x in lst:
                try:
                    f = float(x)
                    res.append(None if (np.isnan(f) or np.isinf(f)) else round(f, 4))
                except Exception:
                    res.append(None)
            return res

        dates = hist.index.strftime('%Y-%m-%d').tolist()
        result = {
            'ticker':        ticker,
            'displayTicker': tw_display(ticker),
            'name':          tw_cn_name(ticker, info.get('longName', info.get('shortName', ticker))),
            'sector':        info.get('sector', ''),
            'industry':      info.get('industry', ''),
            'country':       info.get('country', 'Taiwan'),
            'description':   (info.get('longBusinessSummary', '') or '')[:300],
            'price':         round(price, 2),
            'change':        round(change, 2),
            'changePct':     round(change_pct, 2),
            'open':          round(last_valid(hist['Open']), 2),
            'high':          round(last_valid(hist['High']), 2),
            'low':           round(last_valid(hist['Low']), 2),
            'prevClose':     round(prev, 2),
            'volume':        safe_int(curr_vol),
            'avgVolume':     safe_int(avg_vol),
            'volRatio':      round(vol_ratio, 2),
            'marketCap':     safe_float(info.get('marketCap', 0)),
            'pe':            round(safe_float(info.get('trailingPE',  0)), 2),
            'forwardPe':     round(safe_float(info.get('forwardPE',   0)), 2),
            'eps':           round(safe_float(info.get('trailingEps', 0)), 2),
            'fwdEps':        fwd_eps,
            'beta':          round(safe_float(info.get('beta',        0)), 2),
            'divYield':      round(safe_float(info.get('dividendYield', 0)), 2),
            'sharesOut':     safe_int(info.get('sharesOutstanding', 0)),
            'week52High':    round(week52h, 2),
            'week52Low':     round(week52l, 2),
            'analystTarget': round(safe_float(info.get('targetMeanPrice', 0)), 2),
            'analystHigh':   round(safe_float(info.get('targetHighPrice',  0)), 2),
            'analystLow':    round(safe_float(info.get('targetLowPrice',   0)), 2),
            'recMean':       round(safe_float(info.get('recommendationMean', 3)), 2),
            'numAnalysts':   safe_int(info.get('numberOfAnalystOpinions', 0)),
            'shortRatio':    short_ratio, 'shortPct':    short_pct,
            'profitMargin':  profit_margin, 'grossMargin': gross_margin,
            'roe':           roe, 'debtEquity': debt_equity,
            'instPct':       inst_pct, 'insiderPct': insider_pct,
            'revGrowth':     rev_growth, 'epsGrowth':  eps_growth,
            'ma5':     round(ma5, 2),  'ma20': round(ma20, 2), 'ma60': round(ma60, 2),
            'macdVal': round(macd_v, 2), 'deaVal': round(dea_v, 2), 'macdHist': round(macd_h, 2),
            'rsi':     round(rsi_v, 2),
            'bbUpper': round(bbu, 2), 'bbMid': round(bbm, 2), 'bbLower': round(bbl, 2),
            'bbWidth': bb_width, 'bbPos': bb_pos,
            'levels': levels, 'conclusions': conclusions, 'catalysts': catalysts,
            'risks': risks, 'strategy': strategy, 'returns': returns,
            'investValue': invest_val, 'quarterly': quarterly,
            'isEtf': is_etf, 'etfData': etf_data,
            'dates': dates,
            'ohlcv': {
                'open':   clean(hist['Open'].tolist()),
                'high':   clean(hist['High'].tolist()),
                'low':    clean(hist['Low'].tolist()),
                'close':  clean(hist['Close'].tolist()),
                'volume': [safe_int(x) for x in hist['Volume'].tolist()],
            },
            'ma':        {'ma5': clean(hist['MA5'].tolist()), 'ma20': clean(hist['MA20'].tolist()), 'ma60': clean(hist['MA60'].tolist())},
            'macd':      {'dif': clean(hist['MACD'].tolist()), 'dea': clean(hist['Signal'].tolist()), 'hist': clean(hist['MACDHist'].tolist())},
            'bollinger': {'upper': clean(hist['BB_upper'].tolist()), 'mid': clean(hist['BB_mid'].tolist()), 'lower': clean(hist['BB_lower'].tolist())},
            'rsiSeries': clean(hist['RSI'].tolist()),
        }
        _cache_set(f'tw_stock:{ticker}', result)
        return jsonify(result)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 500


def _fetch_gnews(query, max_results=10):
    try:
        q   = urllib.parse.quote(query)
        url = f'https://news.google.com/rss/search?q={q}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant'
        r   = _requests.get(url, timeout=6, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            return []
        root  = ET.fromstring(r.content)
        items = root.findall('.//item')
        out   = []
        for item in items[:max_results]:
            title = item.findtext('title', '').strip()
            link  = item.findtext('link', '').strip()
            pub   = item.findtext('pubDate', '')
            src   = item.find('source')
            publisher = src.text.strip() if src is not None else 'Google News'
            if title and link:
                out.append({'title': title, 'publisher': publisher, 'url': link,
                            'summary': '', 'pubTime': pub})
        return out
    except Exception:
        return []


@app.route('/api/tw/news/<ticker>')
def get_tw_news(ticker):
    ticker = tw_normalize(ticker)
    cached = _cache_get(f'tw_news:{ticker}')
    if cached: return jsonify(cached)
    try:
        stock    = yf.Ticker(ticker)
        info     = stock.info
        raw_news = stock.news or []
        articles = []
        seen_titles = set()
        for item in raw_news[:12]:
            c         = item.get('content', {})
            title     = c.get('title', '')
            publisher = (c.get('provider') or {}).get('displayName', '')
            url       = (c.get('canonicalUrl') or {}).get('url', '')
            summary   = c.get('summary', '') or ''
            pub_time  = c.get('pubDate', '')
            if title and title not in seen_titles:
                seen_titles.add(title)
                articles.append({'title': title, 'publisher': publisher, 'url': url,
                                  'summary': summary[:180], 'pubTime': pub_time})

        # Supplement with Google News if fewer than 6 articles
        if len(articles) < 6:
            code = ticker.replace('.TW','').replace('.TWO','')
            query = f'{code} 台股'
            gn = _fetch_gnews(query, max_results=12)
            for a in gn:
                if a['title'] not in seen_titles:
                    seen_titles.add(a['title'])
                    articles.append(a)
                    if len(articles) >= 15:
                        break

        result = {'ticker': ticker, 'articles': articles}
        _cache_set(f'tw_news:{ticker}', result, ttl=180)
        return jsonify(result)
    except Exception as e:
        return jsonify({'ticker': ticker, 'articles': [], 'error': str(e)})


@app.route('/api/tw/fundamentals/<ticker>')
def get_tw_fundamentals(ticker):
    ticker = tw_normalize(ticker)
    cached = _cache_get(f'tw_fund:{ticker}')
    if cached: return jsonify(cached)
    try:
        stock = yf.Ticker(ticker)
        info  = stock.info

        ocf_val = fcf_val = 0
        try:
            cf = stock.cashflow
            if cf is not None and not cf.empty:
                for lbl in ['Operating Cash Flow', 'Total Cash From Operating Activities']:
                    if lbl in cf.index:
                        ocf_val = safe_float(cf.loc[lbl].iloc[0]); break
                for lbl in ['Free Cash Flow']:
                    if lbl in cf.index:
                        fcf_val = safe_float(cf.loc[lbl].iloc[0]); break
                if fcf_val == 0 and ocf_val != 0:
                    for lbl in ['Capital Expenditure', 'Capital Expenditures']:
                        if lbl in cf.index:
                            fcf_val = ocf_val + safe_float(cf.loc[lbl].iloc[0]); break
        except Exception:
            pass

        top_holders = []
        try:
            ih = stock.institutional_holders
            if ih is not None and not ih.empty:
                cols = [str(c) for c in ih.columns]
                name_col = next((c for c in cols if 'holder' in c.lower() or 'institution' in c.lower()), None)
                pct_col  = next((c for c in cols if 'pct' in c.lower() or '%' in c or 'out' in c.lower()), None)
                val_col  = next((c for c in cols if 'value' in c.lower()), None)
                if name_col:
                    for _, row in ih.head(5).iterrows():
                        holder = str(row[name_col])
                        if holder and holder != 'nan' and not holder[:4].isdigit():
                            pct = safe_float(row[pct_col]) if pct_col else 0
                            val = safe_float(row[val_col]) if val_col else 0
                            pct_disp = round(pct * 100, 2) if pct < 1 else round(pct, 2)
                            top_holders.append({'holder': holder[:35], 'pct': pct_disp,
                                                'value': round(val / 1e9, 2)})
        except Exception:
            pass

        earnings_date = None
        try:
            cal = stock.calendar
            if cal is not None and not cal.empty:
                col = cal.columns[0]
                earnings_date = str(col.date()) if hasattr(col, 'date') else str(col)[:10]
        except Exception:
            pass

        mktcap    = safe_float(info.get('marketCap', 0))
        fcf_yield = round(fcf_val / mktcap * 100, 2) if mktcap and fcf_val else 0
        result = {
            'ticker':       ticker,
            'ocf':          round(ocf_val / 1e8, 2),
            'fcf':          round(fcf_val / 1e8, 2),
            'fcfYield':     fcf_yield,
            'pfcf':         round(mktcap / fcf_val, 1) if fcf_val and fcf_val > 0 else None,
            'debtEquity':   round(safe_float(info.get('debtToEquity',       0)), 1),
            'currentRatio': round(safe_float(info.get('currentRatio',        0)), 2),
            'roe':          round(safe_float(info.get('returnOnEquity',      0)) * 100, 1),
            'roa':          round(safe_float(info.get('returnOnAssets',      0)) * 100, 1),
            'profitMargin': round(safe_float(info.get('profitMargins',       0)) * 100, 1),
            'grossMargin':  round(safe_float(info.get('grossMargins',        0)) * 100, 1),
            'instPct':      round(safe_float(info.get('heldPercentInstitutions', 0)) * 100, 1),
            'insiderPct':   round(safe_float(info.get('heldPercentInsiders', 0)) * 100, 1),
            'shortRatio':   round(safe_float(info.get('shortRatio',          0)), 1),
            'shortPct':     round(safe_float(info.get('shortPercentOfFloat', 0)) * 100, 2),
            'earningsDate': earnings_date,
            'epsEst':       round(safe_float(info.get('forwardEps',          0)), 2),
            'revGrowth':    round(safe_float(info.get('revenueGrowth',       0)) * 100, 1),
            'epsGrowth':    round(safe_float(info.get('earningsGrowth',      0)) * 100, 1),
            'topHolders':   top_holders,
        }
        _cache_set(f'tw_fund:{ticker}', result)
        return jsonify(result)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/tw/etf/<ticker>')
def get_tw_etf(ticker):
    ticker = tw_normalize(ticker)
    cached = _cache_get(f'tw_etf:{ticker}')
    if cached: return jsonify(cached)
    try:
        stock = yf.Ticker(ticker)
        info  = stock.info

        # ── Dividend history ──
        div_history = []
        div_frequency = '未知'
        div_months = []
        try:
            divs = stock.dividends
            if divs is not None and not divs.empty:
                divs_sorted = divs.sort_index(ascending=False)
                for date, amount in divs_sorted.head(16).items():
                    div_history.append({'date': str(date)[:10], 'amount': round(float(amount), 4)})
                if len(div_history) >= 2:
                    dates = [pd.Timestamp(d['date']) for d in div_history[:10]]
                    gaps  = [(dates[i] - dates[i+1]).days for i in range(len(dates)-1) if i+1 < len(dates)]
                    avg_gap = sum(gaps) / len(gaps) if gaps else 365
                    if   avg_gap < 45:  div_frequency = '月配'
                    elif avg_gap < 100: div_frequency = '季配'
                    elif avg_gap < 200: div_frequency = '半年配'
                    else:               div_frequency = '年配'
                    div_months = sorted(list(set([d.month for d in dates[:8]])))
        except Exception:
            pass

        # ── Top holdings ──
        holdings = []
        try:
            th = stock.funds_top_holdings
            if th is not None and not th.empty:
                cols = [str(c) for c in th.columns]
                sym_col  = next((c for c in cols if 'symbol' in c.lower() or 'ticker' in c.lower()), None)
                name_col = next((c for c in cols if 'name'   in c.lower() or 'holding' in c.lower()), cols[0] if cols else None)
                pct_col  = next((c for c in cols if 'pct'    in c.lower() or 'percent' in c.lower() or 'weight' in c.lower() or 'asset' in c.lower()), None)
                for _, row in th.head(10).iterrows():
                    sym  = str(row[sym_col])  if sym_col  else ''
                    name = str(row[name_col]) if name_col else ''
                    pct  = safe_float(row[pct_col]) if pct_col else 0
                    if pct > 1: pct /= 100
                    if name and name != 'nan':
                        holdings.append({'symbol': sym[:10], 'name': name[:30], 'pct': round(pct * 100, 2)})
        except Exception:
            pass

        # ── NAV & premium/discount ──
        nav          = safe_float(info.get('navPrice', info.get('regularMarketPrice', 0)))
        market_price = safe_float(info.get('currentPrice', info.get('regularMarketPrice', 0)))
        premium_disc = round((market_price / nav - 1) * 100, 3) if nav > 0 else 0

        # ── Next ex-dividend ──
        last_div_date = info.get('lastDividendDate', None)
        ex_div_date   = info.get('exDividendDate',   None)
        for attr in ['last_div_date', 'ex_div_date']:
            val = locals()[attr]
            if val:
                try:
                    locals()[attr] = str(pd.Timestamp(val, unit='s').date())
                except Exception:
                    locals()[attr] = None

        if last_div_date:
            try: last_div_date = str(pd.Timestamp(last_div_date, unit='s').date())
            except Exception: last_div_date = None
        if ex_div_date:
            try: ex_div_date = str(pd.Timestamp(ex_div_date, unit='s').date())
            except Exception: ex_div_date = None

        # ── Annual yield calculation from history ──
        annual_div = 0
        if div_history:
            if div_frequency == '月配':
                annual_div = sum(d['amount'] for d in div_history[:12])
            elif div_frequency == '季配':
                annual_div = sum(d['amount'] for d in div_history[:4])
            elif div_frequency == '半年配':
                annual_div = sum(d['amount'] for d in div_history[:2])
            else:
                annual_div = div_history[0]['amount'] if div_history else 0
        hist_yield = round(annual_div / market_price * 100, 2) if market_price > 0 and annual_div > 0 else 0

        result = {
            'ticker':          ticker,
            'nav':             round(nav, 4),
            'premiumDiscount': premium_disc,
            'lastDividend':    round(safe_float(info.get('lastDividendValue', 0)), 4),
            'lastDividendDate':last_div_date,
            'exDividendDate':  ex_div_date,
            'dividendFrequency': div_frequency,
            'dividendMonths':  div_months,
            'dividendHistory': div_history,
            'histYield':       hist_yield,
            'holdings':        holdings,
            'totalAssets':     round(safe_float(info.get('totalAssets', 0)) / 1e8, 1),
        }
        _cache_set(f'tw_etf:{ticker}', result, ttl=600)
        return jsonify(result)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/tw/realtime/<ticker>')
def get_tw_realtime(ticker):
    ticker = tw_normalize(ticker)
    cached = _cache_get(f'tw_rt:{ticker}')
    if cached: return jsonify(cached)
    try:
        stock = yf.Ticker(ticker)
        info  = stock.info
        price = safe_float(info.get('currentPrice', info.get('regularMarketPrice', 0)))
        prev  = safe_float(info.get('previousClose', info.get('regularMarketPreviousClose', 0)))
        change = price - prev
        change_pct = change / prev * 100 if prev else 0
        result = {
            'ticker':    ticker,
            'price':     round(price, 2),
            'change':    round(change, 2),
            'changePct': round(change_pct, 2),
            'volume':    safe_int(info.get('regularMarketVolume', 0)),
            'high':      round(safe_float(info.get('dayHigh', info.get('regularMarketDayHigh', 0))), 2),
            'low':       round(safe_float(info.get('dayLow',  info.get('regularMarketDayLow',  0))), 2),
        }
        # 查無報價（停牌/下市）以較長 TTL 負向快取，避免每 30 秒又慢慢重抓一次
        _cache_set(f'tw_rt:{ticker}', result, ttl=30 if price else 600)
        return jsonify(result)
    except Exception as e:
        neg = {'ticker': ticker, 'price': 0, 'change': 0, 'changePct': None, 'error': str(e)}
        _cache_set(f'tw_rt:{ticker}', neg, ttl=300)   # 失敗也快取，避免重複逾時拖慢
        return jsonify(neg)


@app.route('/api/tw/signal/<ticker>')
def get_tw_signal(ticker):
    ticker  = tw_normalize(ticker)
    profile = request.args.get('profile', 'steady')
    cached  = _cache_get(f'tw_sig:{ticker}:{profile}')
    if cached: return jsonify(cached)
    try:
        stock = yf.Ticker(ticker)
        info  = stock.info
        price = safe_float(info.get('currentPrice', info.get('regularMarketPrice', 0)))
        name  = info.get('shortName', info.get('longName', ticker))
        result = (_aggressive_signal(stock, ticker, price, name)
                  if profile == 'aggressive'
                  else _steady_signal(stock, ticker, price, name))
        _cache_set(f'tw_sig:{ticker}:{profile}', result, ttl=120)
        return jsonify(result)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/tw/notify/line', methods=['POST'])
def send_line_notify():
    try:
        data    = request.json or {}
        token   = data.get('token', '').strip()
        user_id = data.get('user_id', '').strip()
        message = data.get('message', '').strip()
        if not token or not user_id or not message:
            return jsonify({'error': 'token, user_id and message required'}), 400
        r = _requests.post(
            'https://api.line.me/v2/bot/message/push',
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {token}',
            },
            json={
                'to': user_id,
                'messages': [{'type': 'text', 'text': message}],
            },
            timeout=10
        )
        return jsonify({'status': r.status_code, 'ok': r.status_code == 200,
                        'msg': r.text[:200]})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/tw/line/config', methods=['GET', 'POST'])
def line_config():
    """Store/retrieve LINE credentials server-side so any browser gets them."""
    cfg_file = os.path.join(os.path.dirname(__file__), 'monitor_config.json')
    if request.method == 'POST':
        data = request.json or {}
        with _monitor_lock:
            cfg = _load_monitor_cfg()
            cfg['line_token']   = data.get('line_token', '').strip()
            cfg['line_user_id'] = data.get('line_user_id', '').strip()
            _save_monitor_cfg(cfg)
        return jsonify({'ok': True})
    else:
        with _monitor_lock:
            cfg = _load_monitor_cfg()
        return jsonify({
            'line_token':   cfg.get('line_token', ''),
            'line_user_id': cfg.get('line_user_id', ''),
        })


@app.route('/api/tw/monitor/register', methods=['POST'])
def monitor_register():
    data = request.json or {}
    ticker = tw_normalize(data.get('ticker', '').strip())
    if not ticker:
        return jsonify({'error': 'ticker required'}), 400
    profile      = data.get('profile', 'aggressive')
    line_token   = data.get('line_token', '').strip()
    line_user_id = data.get('line_user_id', '').strip()
    now_str = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M')
    with _monitor_lock:
        cfg = _load_monitor_cfg()
        existing = cfg['tickers'].get(ticker, {})
        cfg['tickers'][ticker] = {
            'profile':          profile,
            'enabled':          existing.get('enabled', True),
            'line_token':       line_token,
            'line_user_id':     line_user_id,
            'last_signal':      existing.get('last_signal'),
            'last_scan':        existing.get('last_scan', ''),
            'last_notify_time': existing.get('last_notify_time', ''),
            'registered_at':    existing.get('registered_at', now_str),
        }
        _save_monitor_cfg(cfg)
    return jsonify({'ok': True, 'ticker': ticker, 'profile': profile})


@app.route('/api/tw/monitor/unregister', methods=['POST'])
def monitor_unregister():
    data = request.json or {}
    ticker = tw_normalize(data.get('ticker', '').strip())
    with _monitor_lock:
        cfg = _load_monitor_cfg()
        cfg['tickers'].pop(ticker, None)
        _save_monitor_cfg(cfg)
    return jsonify({'ok': True, 'ticker': ticker})


@app.route('/api/tw/monitor/list')
def monitor_list():
    with _monitor_lock:
        cfg = _load_monitor_cfg()
    return jsonify({'tickers': cfg.get('tickers', {}),
                    'paused':  bool(cfg.get('monitor_paused', False))})


@app.route('/api/tw/monitor/toggle', methods=['POST'])
def monitor_toggle():
    """個別開關：開啟/關閉某檔監測（關閉者不掃描、不呼叫 AI，省 token）。"""
    data    = request.json or {}
    ticker  = tw_normalize(data.get('ticker', '').strip())
    enabled = bool(data.get('enabled', True))
    with _monitor_lock:
        cfg = _load_monitor_cfg()
        if ticker not in cfg.get('tickers', {}):
            return jsonify({'error': 'ticker not monitored'}), 404
        cfg['tickers'][ticker]['enabled'] = enabled
        _save_monitor_cfg(cfg)
    return jsonify({'ok': True, 'ticker': ticker, 'enabled': enabled})


@app.route('/api/tw/monitor/pause', methods=['POST'])
def monitor_pause():
    """總開關：一鍵暫停／啟用全部監測。"""
    data   = request.json or {}
    paused = bool(data.get('paused', False))
    with _monitor_lock:
        cfg = _load_monitor_cfg()
        cfg['monitor_paused'] = paused
        _save_monitor_cfg(cfg)
    return jsonify({'ok': True, 'paused': paused})


@app.route('/api/tw/monitor/scan_now', methods=['POST'])
def monitor_scan_now():
    threading.Thread(target=_run_server_scan, daemon=True).start()
    return jsonify({'ok': True})


@app.route('/api/tw/intraday/<ticker>')
def get_tw_intraday(ticker):
    ticker = tw_normalize(ticker)
    cached = _cache_get(f'tw_intra:{ticker}')
    if cached: return jsonify(cached)
    try:
        stock = yf.Ticker(ticker)
        hist  = stock.history(period='1d', interval='5m')
        if hist.empty:
            return jsonify({'error': '今日無盤中資料', 'ticker': ticker}), 404

        def clean(lst):
            res = []
            for x in lst:
                try:
                    f = float(x)
                    res.append(None if (np.isnan(f) or np.isinf(f)) else round(f, 4))
                except Exception:
                    res.append(None)
            return res

        dates  = hist.index.strftime('%H:%M').tolist()
        result = {
            'ticker':  ticker,
            'dates':   dates,
            'ohlcv': {
                'open':   clean(hist['Open'].tolist()),
                'high':   clean(hist['High'].tolist()),
                'low':    clean(hist['Low'].tolist()),
                'close':  clean(hist['Close'].tolist()),
                'volume': [safe_int(x) for x in hist['Volume'].tolist()],
            },
        }
        _cache_set(f'tw_intra:{ticker}', result, ttl=60)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/tw/hourly/<ticker>')
def get_tw_hourly(ticker):
    """60分K：最近 60 天小時線 OHLCV"""
    ticker = tw_normalize(ticker)
    cached = _cache_get(f'tw_hourly:{ticker}')
    if cached: return jsonify(cached)
    try:
        stock = yf.Ticker(ticker)
        hist  = stock.history(period='60d', interval='1h')
        if hist.empty:
            return jsonify({'error': '無小時線資料', 'ticker': ticker}), 404

        def clean(lst):
            res = []
            for x in lst:
                try:
                    f = float(x)
                    res.append(None if (np.isnan(f) or np.isinf(f)) else round(f, 2))
                except Exception:
                    res.append(None)
            return res

        dates  = hist.index.strftime('%m/%d %H:%M').tolist()
        close  = hist['Close']
        n      = len(close)
        ma5    = close.rolling(min(5,  n), min_periods=1).mean()
        ma20   = close.rolling(min(20, n), min_periods=1).mean()
        ma60   = close.rolling(min(60, n), min_periods=1).mean()
        macd_s, sig_s, hist_s = calc_macd(close)
        rsi_s  = calc_rsi(close)
        bb_u, bb_m, bb_l = calc_bollinger(close)

        result = {
            'ticker': ticker,
            'dates':  dates,
            'ohlcv': {
                'open':   clean(hist['Open'].tolist()),
                'high':   clean(hist['High'].tolist()),
                'low':    clean(hist['Low'].tolist()),
                'close':  clean(close.tolist()),
                'volume': [safe_int(x) for x in hist['Volume'].tolist()],
            },
            'ma':   { 'ma5': clean(ma5.tolist()), 'ma20': clean(ma20.tolist()), 'ma60': clean(ma60.tolist()) },
            'macd': { 'dif': clean(macd_s.tolist()), 'dea': clean(sig_s.tolist()), 'hist': clean(hist_s.tolist()) },
            'bollinger': { 'upper': clean(bb_u.tolist()), 'mid': clean(bb_m.tolist()), 'lower': clean(bb_l.tolist()) },
            'rsiSeries': clean(rsi_s.tolist()),
        }
        _cache_set(f'tw_hourly:{ticker}', result, ttl=300)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Smart Monitor ─────────────────────────────────────────────────────
def _quick_signal(stock, ticker, price, name, profile='steady'):
    """Lightweight daily-bar signal for monitor scanning."""
    try:
        hist = stock.history(period='3mo', interval='1d')
        if hist.empty or len(hist) < 15:
            return {'action': 'WAIT', 'actionCn': '資料不足', 'confidence': '-',
                    'reason': '歷史資料不足，無法分析', 'stopLoss': 0}
        close = hist['Close']
        n     = len(close)
        ma20  = safe_float(close.rolling(min(20, n)).mean().iloc[-1])
        ma60  = safe_float(close.rolling(min(60, n)).mean().iloc[-1])
        rsi   = safe_float(calc_rsi(close).iloc[-1])
        macd_s, sig_s, hist_s = calc_macd(close)
        macd_v  = safe_float(macd_s.iloc[-1])
        sig_v   = safe_float(sig_s.iloc[-1])
        hist_v  = safe_float(hist_s.iloc[-1])
        hist_pv = safe_float(hist_s.iloc[-2]) if n > 1 else 0
        avg_vol  = safe_float(hist['Volume'].rolling(min(20, n)).mean().iloc[-1])
        curr_vol = last_valid(hist['Volume'])
        vol_ratio = curr_vol / avg_vol if avg_vol > 0 else 1.0
        stop = round(ma20 * 0.97, 2)

        bull = bear = 0
        if price > ma20: bull += 1
        else:            bear += 1
        if price > ma60: bull += 1
        else:            bear += 1
        if macd_v > sig_v and hist_v > hist_pv:   bull += 1
        elif macd_v < sig_v and hist_v < hist_pv: bear += 1
        if rsi < 40:   bull += 1
        elif rsi > 75: bear += 1
        if vol_ratio > 1.5 and price > ma20: bull += 1

        if bull >= 4:   action, cn, conf = 'BUY',  '強烈買進', '高'
        elif bull >= 3: action, cn, conf = 'BUY',  '建議買進', '中'
        elif bear >= 4: action, cn, conf = 'SELL', '建議賣出', '高'
        elif bear >= 3: action, cn, conf = 'SELL', '考慮賣出', '中'
        elif bull >= 2: action, cn, conf = 'WATCH','接近買點', '低'
        else:           action, cn, conf = 'HOLD', '持續觀望', '-'

        parts = []
        parts.append(f'{"站穩" if price > ma20 else "跌破"} MA20(${ma20:.1f})')
        parts.append(f'RSI {rsi:.0f}')
        parts.append(f'MACD {"金叉" if macd_v > sig_v else "死叉"}')
        if vol_ratio >= 1.5: parts.append(f'量比 {vol_ratio:.1f}x')
        return {'action': action, 'actionCn': cn, 'confidence': conf,
                'reason': ' | '.join(parts), 'stopLoss': stop}
    except Exception as e:
        return {'action': 'WAIT', 'actionCn': '分析失敗', 'confidence': '-',
                'reason': str(e)[:60], 'stopLoss': 0}


@app.route('/api/monitor/scan', methods=['POST'])
def monitor_scan():
    data    = request.json or {}
    tickers = [t.upper().strip() for t in data.get('tickers', []) if str(t).strip()][:10]
    profile = data.get('profile', 'steady')
    if not tickers:
        return jsonify([])

    def fetch_one(ticker):
        cache_key = f'mon:{ticker}:{profile}'
        cached = _cache_get(cache_key)
        if cached:
            return cached
        try:
            stock  = yf.Ticker(ticker)
            info   = stock.info
            price  = safe_float(info.get('currentPrice', info.get('regularMarketPrice', 0)))
            if price == 0:
                h2 = stock.history(period='2d')
                if not h2.empty: price = safe_float(h2['Close'].iloc[-1])
            prev    = safe_float(info.get('previousClose', info.get('regularMarketPreviousClose', 0)))
            change  = price - prev
            chg_pct = change / prev * 100 if prev else 0
            name    = (info.get('shortName') or info.get('longName') or ticker)[:25]
            sig     = _quick_signal(stock, ticker, price, name, profile)
            entry   = {
                'ticker':    ticker,
                'name':      name,
                'price':     round(price, 2),
                'change':    round(change, 2),
                'changePct': round(chg_pct, 2),
                **sig,
            }
            _cache_set(cache_key, entry, ttl=90)
            return entry
        except Exception as e:
            return {'ticker': ticker, 'name': ticker, 'price': 0, 'change': 0,
                    'changePct': 0, 'action': 'ERR', 'actionCn': '載入失敗',
                    'confidence': '-', 'reason': str(e)[:60], 'stopLoss': 0}

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(fetch_one, t): t for t in tickers}
        results_map = {}
        for f in as_completed(futures):
            results_map[futures[f]] = f.result()

    return jsonify([results_map[t] for t in tickers if t in results_map])


# ═══════════════════════════════════════════════════════════════════
# SCREENER MODULE
# ═══════════════════════════════════════════════════════════════════

STRATEGIES_FILE = os.path.join(os.path.dirname(__file__), 'strategies.json')
_strat_lock = threading.Lock()

def _load_strategies():
    try:
        if os.path.exists(STRATEGIES_FILE):
            with open(STRATEGIES_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def _save_strategies(s):
    with open(STRATEGIES_FILE, 'w', encoding='utf-8') as f:
        json.dump(s, f, ensure_ascii=False, indent=2)

# ── KD Stochastic (Taiwan standard: K = prev_K*(1-1/m1) + RSV/m1) ──
def calc_kd(high, low, close, n=9, m1=3, m2=3):
    low_n  = low.rolling(n, min_periods=1).min()
    high_n = high.rolling(n, min_periods=1).max()
    denom  = (high_n - low_n).replace(0, np.nan)
    rsv    = ((close - low_n) / denom * 100).fillna(50).clip(0, 100)
    alpha_k = 1.0 / m1
    alpha_d = 1.0 / m2
    k_list, d_list = [], []
    k = d = 50.0
    for r in rsv:
        k = alpha_k * r + (1 - alpha_k) * k
        d = alpha_d * k + (1 - alpha_d) * d
        k_list.append(k)
        d_list.append(d)
    return (pd.Series(k_list, index=close.index),
            pd.Series(d_list, index=close.index))

# ── Stock universe for screener ──
TW_SCREENER_UNIVERSE = {
    '大型指數ETF':  ['0050','006208','00757','0051','00830','006205'],
    '高股息ETF':    ['0056','00878','00713','00919','00929','00930','00918','00900','00939','00940','00944','00946','00953'],
    '科技主題ETF':  ['00662','00646','00770','00881','00830','00893','00905','00911','00912','00913'],
    '半導體':       ['2330','2303','2344','3034','2379','3711','2454','2408','3481','2302','5347','3046','6274','3563','6533','6770'],
    'IC設計':       ['3034','2379','6547','3443','3023','2454','3532','6770','4966','3035','3515','6488','8046','5269','3653'],
    '電子製造':     ['2317','2382','2356','2308','2327','2357','3008','2301','2388','2342','2365','3231','2353','2360','3037','2354','6116'],
    '伺服器AI':     ['2308','2382','3008','6669','6278','2376','3014','4977','6138','5483'],
    '電子零組件':   ['2330','6239','3481','2449','3617','5285','4958','6669','3443','2393','2401'],
    '金融銀行':     ['2886','2884','2881','2882','2892','2885','2887','2891','2880','5876','5871','2823','2816','2812','5880'],
    '保險證券':     ['2882','2881','2884','2889','2890','2834','2823','6005','2888'],
    '傳產塑化':     ['1301','1303','1326','1308','1309','1310','1312','1314','1317','2702'],
    '鋼鐵金屬':     ['2002','2006','2014','2015','9910','2205','2207','2008'],
    '食品飲料':     ['1216','1203','1210','1215','1225','1229','1231','1232','4205','2103'],
    '零售百貨':     ['2912','2915','9904','9940','6505','2707','2723','2728'],
    '電信網路':     ['2412','4904','3045','6803','4977','3515'],
    '能源石化':     ['6505','1590','1605','1609'],
    '航運物流':     ['2603','2609','2615','2610','2618','2612','5608','2616'],
    '生技醫療':     ['4938','4144','6497','1723','4107','6510','4743','1707','1786','6196','4119','4166'],
    '建設營造':     ['2520','2524','2528','5522','5536','2534','1477','2543'],
    '汽車機械':     ['2207','1476','1504','1503','2106','2105','1513','1533','1560'],
    '觀光旅遊':     ['2707','2706','2727','2722','2701','5603'],
}

US_SCREENER_UNIVERSE = {
    '科技巨頭':   ['AAPL','MSFT','GOOGL','GOOG','META','AMZN','NVDA','TSLA','ORCL','IBM','ADBE','NOW','INTU','PANW','CRWD'],
    '半導體':     ['NVDA','AMD','INTC','QCOM','MU','AVGO','TSM','AMAT','LRCX','KLAC','MRVL','ON','TXN','ADI','NXPI','MPWR','WOLF','ONTO','ENTG','AEHR'],
    '雲端AI':     ['MSFT','AMZN','GOOGL','CRM','SNOW','PLTR','AI','NET','DDOG','ZS','OKTA','MDB','GTLB','HUBS','DOCN','CFLT','TTD','NCNO'],
    '軟體SaaS':   ['ADBE','NOW','INTU','WDAY','VEEV','COUP','ZM','DOCU','TWLO','BILL','PAYC','WEX','PCOR','SMAR','BRZE'],
    '金融銀行':   ['JPM','BAC','GS','MS','WFC','C','USB','PNC','TFC','COF','AXP','V','MA','PYPL','SQ','FIS','FI'],
    '保險資產':   ['BRK-B','AIG','MET','PRU','AFL','ALL','CB','TRV','HIG','PGR','BLK','SCHW','IBKR'],
    '醫療生技':   ['JNJ','UNH','PFE','MRNA','ABBV','BMY','LLY','AMGN','GILD','REGN','VRTX','BIIB','ISRG','MDT','BSX','EW','ZBH','DXCM','PODD'],
    '醫療服務':   ['CVS','MCK','CAH','ABC','HCA','THC','CNC','MOH','HUM','ELV','CI'],
    '消費零售':   ['AMZN','WMT','COST','NKE','MCD','SBUX','TGT','HD','LOW','LULU','ROST','TJX','ULTA','RH','EBAY'],
    '消費品牌':   ['KO','PEP','PG','CL','EL','MDLZ','GIS','K','CPB','HSY','MKC','CHD','CLX'],
    '媒體娛樂':   ['NFLX','DIS','CMCSA','WBD','PARA','SIRI','SPOT','LYV','IMAX','AMC'],
    '電動車':     ['TSLA','RIVN','LCID','NIO','LI','XPEV','FSR','NKLA','RIDE','GOEV'],
    '能源石油':   ['XOM','CVX','COP','SLB','HAL','BKR','EOG','PXD','MPC','VLO','PSX','OXY','DVN','FANG'],
    '再生能源':   ['ENPH','SEDG','PLUG','FCEL','BE','NOVA','RUN','NEE','BEP','CWEN','AES','ARRY'],
    '航空航運':   ['DAL','UAL','AAL','LUV','ALK','JBLU','FDX','UPS','XPO','CHRW','EXPD','GXO'],
    '汽車製造':   ['F','GM','STLA','HMC','TM','RIVN','TSLA','LCID'],
    '房地產REIT': ['AMT','PLD','EQIX','O','SPG','VICI','WELL','DLR','CCI','PSA','EXR','AVB','EQR','MAA','UDR'],
    '工業製造':   ['HON','GE','MMM','CAT','DE','EMR','ETN','ROK','ITW','PH','DHR','AME','ROP','XYL'],
    '航太國防':   ['LMT','RTX','BA','NOC','GD','L3H','LDOS','CACI','SAIC','HII'],
    '材料化工':   ['LIN','APD','SHW','NEM','FCX','AA','NUE','CLF','CF','MOS','ALB','MP','LYFT'],
    '電信通訊':   ['T','VZ','TMUS','LUMN','DISH','CCOI','SHEN'],
    '大型ETF':    ['SPY','QQQ','IWM','DIA','VTI','VOO','SCHB'],
    '主題ETF':    ['GLD','SLV','TLT','HYG','LQD','ARKK','ARKG','ARKF','BOTZ','ROBO','HERO','ESPO'],
    '槓桿ETF':    ['TQQQ','SOXL','UPRO','LABU','FNGU','SOXS','SPXS'],
}

# ── 技術指標輔助函式 ──────────────────────────────────────────────────
def calc_william_r(high, low, close, period=14):
    hh = high.rolling(period).max()
    ll = low.rolling(period).min()
    return (hh - close) / (hh - ll).replace(0, np.nan) * -100

def calc_cci(high, low, close, period=20):
    tp = (high + low + close) / 3
    ma = tp.rolling(period).mean()
    md = tp.rolling(period).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
    return (tp - ma) / (0.015 * md.replace(0, np.nan))

def calc_adx(high, low, close, period=14):
    tr = pd.concat([high - low,
                    (high - close.shift()).abs(),
                    (low  - close.shift()).abs()], axis=1).max(axis=1)
    atr = tr.ewm(span=period, adjust=False).mean()
    dm_plus  = (high - high.shift()).clip(lower=0)
    dm_minus = (low.shift()  - low ).clip(lower=0)
    dm_plus  = dm_plus.where(dm_plus > dm_minus, 0)
    dm_minus = dm_minus.where(dm_minus > dm_plus, 0)
    di_plus  = dm_plus.ewm(span=period, adjust=False).mean()  / atr.replace(0, np.nan) * 100
    di_minus = dm_minus.ewm(span=period, adjust=False).mean() / atr.replace(0, np.nan) * 100
    dx = ((di_plus - di_minus).abs() / (di_plus + di_minus).replace(0, np.nan)) * 100
    adx = dx.ewm(span=period, adjust=False).mean()
    return adx, di_plus, di_minus

def calc_bias(close, period=20):
    ma = close.rolling(period).mean()
    return (close - ma) / ma.replace(0, np.nan) * 100

def calc_psy(close, period=12):
    up = (close.diff() > 0).astype(int)
    return up.rolling(period).sum() / period * 100

def calc_atr(high, low, close, period=14):
    tr = pd.concat([high - low,
                    (high - close.shift()).abs(),
                    (low  - close.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()


# ── FinMind 逐檔籌碼來源（本機被 TWSE 封鎖時的替代資料源）──────────────────
#   TWSE openapi/www 從本機 IP 連不到（同中文名問題），改用 FinMind 逐檔取。
#   免費匿名查詢需帶 data_id、且有限流，故採「每檔分開快取」，個股/持倉頁可正常顯示籌碼。
_finmind_token_cache = {'v': None, 'loaded': False}

def _finmind_token() -> str:
    """FinMind token：優先環境變數，其次設定檔（register 等級可拉高逐檔查詢額度）。"""
    t = os.environ.get('FINMIND_TOKEN')
    if t:
        return t.strip()
    if not _finmind_token_cache['loaded']:
        try:
            _finmind_token_cache['v'] = (_load_agent_cfg().get('finmind_token') or '').strip()
        except Exception:
            _finmind_token_cache['v'] = ''
        _finmind_token_cache['loaded'] = True
    return _finmind_token_cache['v'] or ''


def _finmind_fetch(dataset: str, data_id: str, days: int = 14) -> list:
    """向 FinMind 取單一股票近 N 日某資料集，回 list（失敗回 []）。"""
    import urllib.request, datetime
    end   = datetime.date.today()
    start = end - datetime.timedelta(days=days)
    url = (f'https://api.finmindtrade.com/api/v4/data?dataset={dataset}'
           f'&data_id={data_id}&start_date={start}&end_date={end}')
    tok = _finmind_token()
    if tok:
        url += f'&token={tok}'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        d = json.loads(urllib.request.urlopen(req, timeout=20).read())
        return d.get('data', []) or []
    except Exception as e:
        print(f'[FinMind] {dataset} {data_id} error: {e}')
        return []


def _inst_net_by_category(rows_one_day: list) -> dict:
    """把 FinMind 某日某股的各類投資人 buy/sell 彙總成外資/投信/自營/合計。"""
    buy  = {}
    sell = {}
    for r in rows_one_day:
        nm = r.get('name', '')
        buy[nm]  = buy.get(nm, 0)  + safe_float(r.get('buy', 0))
        sell[nm] = sell.get(nm, 0) + safe_float(r.get('sell', 0))
    net = lambda nm: buy.get(nm, 0) - sell.get(nm, 0)
    foreign = net('Foreign_Investor') + net('Foreign_Dealer_Self')
    trust   = net('Investment_Trust')
    dealer  = net('Dealer_self') + net('Dealer_Hedging')
    return {
        'foreign_net': foreign, 'trust_net': trust, 'dealer_net': dealer,
        'total_net':   foreign + trust + dealer,
        'foreign_buy':  buy.get('Foreign_Investor', 0) + buy.get('Foreign_Dealer_Self', 0),
        'foreign_sell': sell.get('Foreign_Investor', 0) + sell.get('Foreign_Dealer_Self', 0),
        'trust_buy':  buy.get('Investment_Trust', 0),
        'trust_sell': sell.get('Investment_Trust', 0),
    }


# ── TWSE 三大法人快取（改用 FinMind 逐檔）──────────────────────────────────
_tw_inst_cache: dict = {}        # code -> (ts, dict|None)
_tw_inst_hist_code_cache: dict = {}   # code -> (ts, dict|None)
_tw_margin_code_cache: dict = {}      # code -> (ts, dict|None)
_tw_lending_code_cache: dict = {}     # code -> (ts, dict|None) 借券賣出餘額
_tw_daytrade_code_cache: dict = {}    # code -> (ts, dict|None) 當沖量
_tw_inst_lock  = threading.Lock()

def _load_tw_inst():
    """（已停用：TWSE 從本機被封鎖，改走 FinMind 逐檔 _get_tw_inst）"""
    import urllib.request, datetime
    try:
        today = datetime.date.today().strftime('%Y%m%d')
        url = f'https://www.twse.com.tw/fund/T86?date={today}&selectType=ALLBUT0999&response=json'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        raw = json.loads(urllib.request.urlopen(req, timeout=12).read())
        if raw.get('stat') != 'OK': return {}
        result = {}
        for row in raw.get('data', []):
            code = str(row[0]).strip()
            def _n(s): return safe_float(str(s).replace(',','').replace(' ',''))
            result[code] = {
                'foreign_net':  _n(row[4]),   # 外資買賣超
                'trust_net':    _n(row[10]),   # 投信買賣超
                'dealer_net':   _n(row[11]),   # 自營商買賣超
                'total_net':    _n(row[18]),   # 三大法人合計
                'foreign_buy':  _n(row[2]),
                'foreign_sell': _n(row[3]),
                'trust_buy':    _n(row[8]),
                'trust_sell':   _n(row[9]),
            }
        with _tw_inst_lock:
            _tw_inst_cache['data'] = result
            _tw_inst_cache['ts']   = time.time()
        return result
    except Exception as e:
        print(f'[Inst] load error: {e}')
        return {}

def _get_tw_inst(code: str):
    """回傳單一股票最近交易日的三大法人買賣超（FinMind 逐檔，每檔快取 1 小時）。"""
    code = str(code).strip().replace('.TW', '').replace('.TWO', '')
    now  = time.time()
    with _tw_inst_lock:
        ent = _tw_inst_cache.get(code)
    if ent and now - ent[0] < 3600:
        return ent[1]
    rows = _finmind_fetch('TaiwanStockInstitutionalInvestorsBuySell', code, days=14)
    res = None
    if rows:
        last = max(r.get('date', '') for r in rows)
        res = _inst_net_by_category([r for r in rows if r.get('date') == last])
    with _tw_inst_lock:
        _tw_inst_cache[code] = (now, res)
    return res


# ── TWSE 三大法人「多日」歷史快取（供連N日買超）────────────────────────
_tw_inst_hist_cache: dict = {}
_tw_inst_hist_lock  = threading.Lock()

def _load_tw_inst_history(days: int = 6):
    """抓近 N 個交易日的 T86，組成每檔股票的法人買賣超序列（最新在前）。快取 4 小時。"""
    import urllib.request, datetime
    result: dict = {}
    collected = 0
    today = datetime.date.today()
    def _n(s): return safe_float(str(s).replace(',', '').replace(' ', ''))

    def _fetch_day(ymd):
        """回傳該日 T86 data list；非交易日回 None；網路錯誤重試後仍失敗回 'ERR'。"""
        url = f'https://www.twse.com.tw/fund/T86?date={ymd}&selectType=ALLBUT0999&response=json'
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                raw = json.loads(urllib.request.urlopen(req, timeout=12).read())
                if raw.get('stat') == 'OK':
                    return raw.get('data', [])
                return None   # 明確非交易日（假日）
            except Exception:
                time.sleep(0.6)  # 多半是被限流，稍候重試
        return 'ERR'

    # 由今天往回逐「日曆日」掃，跳過週末，直到湊滿 N 個交易日。
    # 為確保「連續日」語意正確：遇到網路錯誤(ERR)直接中止，不以更早的日期頂替造成跳日。
    for back in range(0, days + 12):
        if collected >= days:
            break
        day = today - datetime.timedelta(days=back)
        if day.weekday() >= 5:   # 5=六 6=日
            continue
        data = _fetch_day(day.strftime('%Y%m%d'))
        if data == 'ERR':
            break          # 寧可資料少也不要跳日
        if data is None:
            continue       # 假日，往前一天
        for row in data:
            if len(row) < 19:
                continue
            code = str(row[0]).strip()
            rec = result.setdefault(code, {'foreign': [], 'trust': [], 'dealer': [], 'total': []})
            rec['foreign'].append(_n(row[4]))
            rec['trust'].append(_n(row[10]))
            rec['dealer'].append(_n(row[11]))
            rec['total'].append(_n(row[18]))
        collected += 1
        time.sleep(0.5)    # 禮貌性間隔，降低被限流機率（此載入每 4 小時才一次）
    if result:
        with _tw_inst_hist_lock:
            _tw_inst_hist_cache['data'] = result
            _tw_inst_hist_cache['ts']   = time.time()
    return result

def _get_tw_inst_hist(code: str):
    """回傳單一股票近 N 日法人買賣超序列（最新在前），FinMind 逐檔，每檔快取 4 小時。"""
    code = str(code).strip().replace('.TW', '').replace('.TWO', '')
    now  = time.time()
    with _tw_inst_hist_lock:
        ent = _tw_inst_hist_code_cache.get(code)
    if ent and now - ent[0] < 14400:
        return ent[1]
    rows = _finmind_fetch('TaiwanStockInstitutionalInvestorsBuySell', code, days=16)
    res = None
    if rows:
        by_date = {}
        for r in rows:
            by_date.setdefault(r.get('date', ''), []).append(r)
        res = {'foreign': [], 'trust': [], 'dealer': [], 'total': []}
        for d in sorted(by_date, reverse=True):   # 最新在前
            agg = _inst_net_by_category(by_date[d])
            res['foreign'].append(agg['foreign_net'])
            res['trust'].append(agg['trust_net'])
            res['dealer'].append(agg['dealer_net'])
            res['total'].append(agg['total_net'])
    with _tw_inst_hist_lock:
        _tw_inst_hist_code_cache[code] = (now, res)
    return res


# ── TWSE 融資融券快取 ──────────────────────────────────────────────────
_tw_margin_cache: dict = {}
_tw_margin_lock  = threading.Lock()

def _load_tw_margin():
    """抓 TWSE 今日融資融券資料，快取 1 小時"""
    import urllib.request
    try:
        req = urllib.request.Request(
            'https://openapi.twse.com.tw/v1/marginTrading/MI_MARGN',
            headers={'User-Agent': 'Mozilla/5.0'})
        data = json.loads(urllib.request.urlopen(req, timeout=12).read())
        result = {}
        for row in data:
            code = str(row.get('股票代號', '')).strip()
            if not code: continue
            result[code] = {
                'margin_today':  safe_float(row.get('融資今日餘額', 0)),
                'margin_prev':   safe_float(row.get('融資前日餘額', 0)),
                'short_today':   safe_float(row.get('融券今日餘額', 0)),
                'short_prev':    safe_float(row.get('融券前日餘額', 0)),
                'margin_buy':    safe_float(row.get('融資買進', 0)),
                'margin_sell':   safe_float(row.get('融資賣出', 0)),
                'short_buy':     safe_float(row.get('融券買進', 0)),
                'short_sell':    safe_float(row.get('融券賣出', 0)),
            }
        with _tw_margin_lock:
            _tw_margin_cache['data'] = result
            _tw_margin_cache['ts']   = time.time()
        return result
    except Exception as e:
        print(f'[Margin] load error: {e}')
        return {}

def _get_tw_margin(code: str):
    """回傳單一股票最近交易日融資融券（FinMind 逐檔，每檔快取 1 小時），找不到回 None。"""
    code = str(code).strip().replace('.TW', '').replace('.TWO', '')
    now  = time.time()
    with _tw_margin_lock:
        ent = _tw_margin_code_cache.get(code)
    if ent and now - ent[0] < 3600:
        return ent[1]
    rows = _finmind_fetch('TaiwanStockMarginPurchaseShortSale', code, days=12)
    res = None
    if rows:
        last = max(rows, key=lambda r: r.get('date', ''))
        res = {
            'margin_today': safe_float(last.get('MarginPurchaseTodayBalance', 0)),
            'margin_prev':  safe_float(last.get('MarginPurchaseYesterdayBalance', 0)),
            'short_today':  safe_float(last.get('ShortSaleTodayBalance', 0)),
            'short_prev':   safe_float(last.get('ShortSaleYesterdayBalance', 0)),
            'margin_buy':   safe_float(last.get('MarginPurchaseBuy', 0)),
            'margin_sell':  safe_float(last.get('MarginPurchaseSell', 0)),
            'short_buy':    safe_float(last.get('ShortSaleBuy', 0)),
            'short_sell':   safe_float(last.get('ShortSaleSell', 0)),
        }
    with _tw_margin_lock:
        _tw_margin_code_cache[code] = (now, res)
    return res


def _get_tw_lending(code: str):
    """借券賣出餘額（FinMind TaiwanDailyShortSaleBalances 的 SBL，逐檔快取 1 小時）。"""
    code = str(code).strip().replace('.TW', '').replace('.TWO', '')
    now  = time.time()
    with _tw_margin_lock:
        ent = _tw_lending_code_cache.get(code)
    if ent and now - ent[0] < 3600:
        return ent[1]
    rows = _finmind_fetch('TaiwanDailyShortSaleBalances', code, days=12)
    res = None
    if rows:
        rows_sorted = sorted(rows, key=lambda r: r.get('date', ''))
        last = rows_sorted[-1]
        prev = rows_sorted[-2] if len(rows_sorted) > 1 else last
        res = {
            'lending_balance':      safe_float(last.get('SBLShortSalesCurrentDayBalance', 0)),
            'lending_balance_prev': safe_float(prev.get('SBLShortSalesCurrentDayBalance', 0)),
            'lending_sell':         safe_float(last.get('SBLShortSalesShortSales', 0)),
        }
    with _tw_margin_lock:
        _tw_lending_code_cache[code] = (now, res)
    return res


_tw_holding_code_cache: dict = {}     # code -> (ts, dict|None) 外資持股比例

def _get_tw_foreign_holding(code: str):
    """外資持股比例（FinMind TaiwanStockShareholding，逐檔快取 4 小時）。"""
    code = str(code).strip().replace('.TW', '').replace('.TWO', '')
    now  = time.time()
    with _tw_margin_lock:
        ent = _tw_holding_code_cache.get(code)
    if ent and now - ent[0] < 14400:
        return ent[1]
    rows = _finmind_fetch('TaiwanStockShareholding', code, days=20)
    res = None
    if rows:
        last = max(rows, key=lambda r: r.get('date', ''))
        res = {'foreign_ratio': safe_float(last.get('ForeignInvestmentSharesRatio', 0))}
    with _tw_margin_lock:
        _tw_holding_code_cache[code] = (now, res)
    return res


def _get_tw_daytrade(code: str):
    """當沖量（FinMind TaiwanStockDayTrading，回最近有量的一日 {date, volume}，逐檔快取 1 小時）。
    當沖比由呼叫端用『當沖量 / 該日總量』算（總量取自價量歷史）。"""
    code = str(code).strip().replace('.TW', '').replace('.TWO', '')
    now  = time.time()
    with _tw_margin_lock:
        ent = _tw_daytrade_code_cache.get(code)
    if ent and now - ent[0] < 3600:
        return ent[1]
    rows = _finmind_fetch('TaiwanStockDayTrading', code, days=12)
    res = None
    valid = [r for r in rows if safe_float(r.get('Volume', 0)) > 0]
    if valid:
        last = max(valid, key=lambda r: r.get('date', ''))
        res = {'date': last.get('date', ''), 'volume': safe_float(last.get('Volume', 0))}
    with _tw_margin_lock:
        _tw_daytrade_code_cache[code] = (now, res)
    return res


_tw_monthrev_code_cache: dict = {}    # code -> (ts, dict|None) 月營收
_tw_eps_code_cache: dict = {}         # code -> (ts, dict|None) EPS／季獲利

def _get_tw_month_revenue(code: str):
    """近期月營收（FinMind TaiwanStockMonthRevenue，逐檔快取 6 小時）。
    回傳最新月營收與 YoY（前年同月比）、MoM（前月比），找不到回 None。
    FinMind 欄位：revenue（當月營收）、revenue_month、revenue_year。"""
    code = str(code).strip().replace('.TW', '').replace('.TWO', '')
    now  = time.time()
    with _tw_margin_lock:
        ent = _tw_monthrev_code_cache.get(code)
    if ent and now - ent[0] < 21600:
        return ent[1]
    # 取近 400 天涵蓋至少 13 個月，才能算 YoY（去年同月）
    rows = _finmind_fetch('TaiwanStockMonthRevenue', code, days=420)
    res = None
    if rows:
        # 以 (year, month) 排序，最新在後
        def _ym(r):
            return (int(r.get('revenue_year', 0)), int(r.get('revenue_month', 0)))
        rows_sorted = sorted([r for r in rows if r.get('revenue') is not None], key=_ym)
        if rows_sorted:
            last = rows_sorted[-1]
            ly, lm = _ym(last)
            cur_rev = safe_float(last.get('revenue', 0))
            prev_m  = rows_sorted[-2] if len(rows_sorted) > 1 else None
            yoy_row = next((r for r in rows_sorted if _ym(r) == (ly - 1, lm)), None)
            mom = (round((cur_rev / safe_float(prev_m.get('revenue', 0)) - 1) * 100, 1)
                   if prev_m and safe_float(prev_m.get('revenue', 0)) else None)
            yoy = (round((cur_rev / safe_float(yoy_row.get('revenue', 0)) - 1) * 100, 1)
                   if yoy_row and safe_float(yoy_row.get('revenue', 0)) else None)
            res = {
                'month':   f'{ly}-{lm:02d}',
                'revenue': cur_rev,            # 單位：元
                'yoy':     yoy,                # %
                'mom':     mom,                # %
            }
    with _tw_margin_lock:
        _tw_monthrev_code_cache[code] = (now, res)
    return res


def _get_tw_eps(code: str):
    """最新季度 EPS／稅後純益（FinMind TaiwanStockFinancialStatements，逐檔快取 12 小時）。
    type='EPS' 取每股盈餘、type='IncomeAfterTaxes' 取稅後淨利，找不到回 None。"""
    code = str(code).strip().replace('.TW', '').replace('.TWO', '')
    now  = time.time()
    with _tw_margin_lock:
        ent = _tw_eps_code_cache.get(code)
    if ent and now - ent[0] < 43200:
        return ent[1]
    # 涵蓋約 5 季
    rows = _finmind_fetch('TaiwanStockFinancialStatements', code, days=500)
    res = None
    if rows:
        eps_rows = [r for r in rows if r.get('type') == 'EPS' and r.get('value') is not None]
        if eps_rows:
            last = max(eps_rows, key=lambda r: r.get('date', ''))
            ds   = last.get('date', '')           # 例 2025-03-31（季底）
            net_rows = [r for r in rows
                        if r.get('type') == 'IncomeAfterTaxes' and r.get('date') == ds]
            res = {
                'quarter':   ds,
                'eps':       safe_float(last.get('value', 0)),
                'net_income': safe_float(net_rows[0].get('value', 0)) if net_rows else None,
            }
    with _tw_margin_lock:
        _tw_eps_code_cache[code] = (now, res)
    return res


def _eval_condition(hist, info, cond, extra=None):
    """Evaluate a single condition. Returns (passed:bool, detail:str).
    extra = {'weekly': DataFrame, 'monthly': DataFrame, 'margin': dict}
    """
    ctype  = cond.get('type', '')
    params = cond.get('params', {})
    close  = hist['Close']
    high   = hist['High']
    low    = hist['Low']
    vol    = hist['Volume']
    n      = len(close)
    price  = last_valid(close)

    def _ma(period):
        return close.rolling(min(int(period), n), min_periods=1).mean()

    try:
        # ── 均線條件 ──────────────────────────────────────
        if ctype == 'price_above_ma':
            period = int(params.get('period', 20))
            ma = safe_float(_ma(period).iloc[-1])
            return price > ma, f'收盤 {price:.2f} > MA{period} {ma:.2f}'

        elif ctype == 'price_below_ma':
            period = int(params.get('period', 20))
            ma = safe_float(_ma(period).iloc[-1])
            return price < ma, f'收盤 {price:.2f} < MA{period} {ma:.2f}'

        elif ctype == 'price_cross_above_ma':
            period    = int(params.get('period', 60))
            within    = int(params.get('within_days', 5))
            ma_series = _ma(period)
            if n < within + 2:
                return False, '資料不足'
            # 最新收盤站上均線，且 within 天前有在均線下
            curr_above = last_valid(close) > last_valid(ma_series)
            was_below  = (close.iloc[-(within+1):-1].values <
                          ma_series.iloc[-(within+1):-1].values).any()
            return (curr_above and was_below,
                    f'近{within}天突破 MA{period} {safe_float(last_valid(ma_series)):.2f}')

        elif ctype == 'price_cross_below_ma':
            period    = int(params.get('period', 20))
            within    = int(params.get('within_days', 3))
            ma_series = _ma(period)
            if n < within + 2:
                return False, '資料不足'
            curr_below = last_valid(close) < last_valid(ma_series)
            was_above  = (close.iloc[-(within+1):-1].values >
                          ma_series.iloc[-(within+1):-1].values).any()
            return (curr_below and was_above,
                    f'近{within}天跌破 MA{period} {safe_float(last_valid(ma_series)):.2f}')

        elif ctype == 'price_below_ma_for_months':
            period = int(params.get('period', 60))
            months = int(params.get('months', 3))
            days   = months * 21
            ma_series = _ma(period)
            if n < days + 5:
                return False, '歷史資料不足'
            window_close = close.iloc[-days:-1]
            window_ma    = ma_series.iloc[-days:-1]
            below_ratio  = (window_close.values < window_ma.values).mean()
            passed = below_ratio >= 0.70 and last_valid(close) >= last_valid(ma_series) * 0.98
            return passed, f'過去{months}月 {below_ratio*100:.0f}% 時間低於 MA{period}'

        elif ctype == 'ma_trending_up':
            period     = int(params.get('period', 60))
            trend_days = int(params.get('trend_days', 5))
            ma_series  = _ma(period)
            if n < trend_days + 2:
                return False, '資料不足'
            return (safe_float(last_valid(ma_series)) > safe_float(ma_series.iloc[-trend_days]),
                    f'MA{period} {trend_days}天持續上揚')

        # ── KD 指標 ───────────────────────────────────────
        elif ctype == 'kd_k_above':
            kn  = int(params.get('kd_n', 9))
            m1  = int(params.get('kd_m1', 3))
            m2  = int(params.get('kd_m2', 3))
            thr = float(params.get('threshold', 50))
            k, _ = calc_kd(high, low, close, kn, m1, m2)
            kv   = safe_float(k.iloc[-1])
            return kv > thr, f'K({kn},{m1},{m2}) = {kv:.1f} > {thr}'

        elif ctype == 'kd_k_below':
            kn  = int(params.get('kd_n', 9))
            m1  = int(params.get('kd_m1', 3))
            m2  = int(params.get('kd_m2', 3))
            thr = float(params.get('threshold', 20))
            k, _ = calc_kd(high, low, close, kn, m1, m2)
            kv   = safe_float(k.iloc[-1])
            return kv < thr, f'K({kn},{m1},{m2}) = {kv:.1f} < {thr}'

        elif ctype == 'kd_golden_cross':
            kn     = int(params.get('kd_n', 9))
            m1     = int(params.get('kd_m1', 3))
            m2     = int(params.get('kd_m2', 3))
            within = int(params.get('within_days', 3))
            k, d   = calc_kd(high, low, close, kn, m1, m2)
            passed = False
            for i in range(-within, 0):
                if (i-1) >= -n and k.iloc[i] > d.iloc[i] and k.iloc[i-1] <= d.iloc[i-1]:
                    passed = True; break
            kv = safe_float(k.iloc[-1])
            return passed, f'KD({kn}) 近{within}天金叉，K={kv:.1f}'

        elif ctype == 'kd_death_cross':
            kn     = int(params.get('kd_n', 9))
            m1     = int(params.get('kd_m1', 3))
            m2     = int(params.get('kd_m2', 3))
            within = int(params.get('within_days', 3))
            k, d   = calc_kd(high, low, close, kn, m1, m2)
            passed = False
            for i in range(-within, 0):
                if (i-1) >= -n and k.iloc[i] < d.iloc[i] and k.iloc[i-1] >= d.iloc[i-1]:
                    passed = True; break
            return passed, f'KD({kn}) 近{within}天死叉'

        # ── MACD 指標 ─────────────────────────────────────
        elif ctype == 'macd_bullish':
            macd_s, sig_s, _ = calc_macd(close)
            mv, sv = safe_float(macd_s.iloc[-1]), safe_float(sig_s.iloc[-1])
            return mv > sv, f'DIF {mv:.4f} > DEA {sv:.4f}'

        elif ctype == 'macd_golden_cross':
            within = int(params.get('within_days', 3))
            macd_s, sig_s, _ = calc_macd(close)
            passed = False
            for i in range(-within, 0):
                if (i-1) >= -n and macd_s.iloc[i] > sig_s.iloc[i] and macd_s.iloc[i-1] <= sig_s.iloc[i-1]:
                    passed = True; break
            return passed, f'MACD 近{within}天金叉'

        elif ctype == 'macd_death_cross':
            within = int(params.get('within_days', 3))
            macd_s, sig_s, _ = calc_macd(close)
            passed = False
            for i in range(-within, 0):
                if (i-1) >= -n and macd_s.iloc[i] < sig_s.iloc[i] and macd_s.iloc[i-1] >= sig_s.iloc[i-1]:
                    passed = True; break
            return passed, f'MACD 近{within}天死叉'

        # ── RSI ────────────────────────────────────────────
        elif ctype == 'rsi_above':
            period = int(params.get('period', 14))
            thr    = float(params.get('threshold', 50))
            rv     = safe_float(calc_rsi(close, period).iloc[-1])
            return rv > thr, f'RSI({period}) = {rv:.1f} > {thr}'

        elif ctype == 'rsi_below':
            period = int(params.get('period', 14))
            thr    = float(params.get('threshold', 30))
            rv     = safe_float(calc_rsi(close, period).iloc[-1])
            return rv < thr, f'RSI({period}) = {rv:.1f} < {thr}'

        # ── 成交量 ─────────────────────────────────────────
        elif ctype == 'volume_ratio_above':
            avg_days = int(params.get('avg_days', 20))
            ratio    = float(params.get('ratio', 1.5))
            avg_vol  = safe_float(vol.rolling(avg_days, min_periods=1).mean().iloc[-1])
            curr_vol = safe_float(vol.iloc[-1])
            vr = curr_vol / avg_vol if avg_vol > 0 else 0
            return vr >= ratio, f'量比 {vr:.2f}x ≥ {ratio}x'

        elif ctype == 'volume_shrinking':
            avg_days    = int(params.get('avg_days', 20))
            recent_days = int(params.get('recent_days', 5))
            older_vol  = safe_float(vol.iloc[-(avg_days):-recent_days].mean())
            recent_vol = safe_float(vol.iloc[-recent_days:].mean())
            ratio      = recent_vol / older_vol if older_vol > 0 else 1
            return ratio < 0.85, f'量縮比 {ratio:.2f}（< 0.85）'

        # ── 布林通道 ───────────────────────────────────────
        elif ctype == 'price_near_bb_lower':
            pct = float(params.get('pct', 5))
            bb_u, bb_m, bb_l = calc_bollinger(close)
            bbl = safe_float(bb_l.iloc[-1])
            dist = (price - bbl) / bbl * 100 if bbl > 0 else 999
            return dist <= pct, f'距布林下軌 {dist:.1f}% ≤ {pct}%'

        elif ctype == 'price_near_bb_upper':
            pct = float(params.get('pct', 3))
            bb_u, bb_m, bb_l = calc_bollinger(close)
            bbu = safe_float(bb_u.iloc[-1])
            dist = (bbu - price) / bbu * 100 if bbu > 0 else 999
            return dist <= pct, f'距布林上軌 {dist:.1f}% ≤ {pct}%'

        # ── 機構籌碼 ───────────────────────────────────────
        elif ctype == 'inst_pct_above':
            thr      = float(params.get('threshold', 40))
            inst_pct = safe_float(info.get('heldPercentInstitutions', 0)) * 100
            return inst_pct >= thr, f'機構持股 {inst_pct:.1f}% ≥ {thr}%'

        elif ctype == 'price_change_above':
            thr = float(params.get('threshold', 3))
            prev = safe_float(close.dropna().iloc[-2]) if len(close.dropna()) > 1 else price
            chg_pct = (price / prev - 1) * 100 if prev else 0
            return chg_pct >= thr, f'今日漲幅 {chg_pct:.2f}% ≥ {thr}%'

        elif ctype == 'price_from_high_below':
            thr = float(params.get('threshold', 20))
            peak = safe_float(hist['High'].rolling(min(252, n)).max().iloc[-1])
            dist = (peak - price) / peak * 100 if peak > 0 else 0
            return dist <= thr, f'距52週高 {dist:.1f}% ≤ {thr}%'

        elif ctype == 'price_range':
            min_p = float(params.get('min', 0))
            max_p = float(params.get('max', 99999))
            return min_p <= price <= max_p, f'股價 {price:.2f} 在 {min_p}~{max_p}'

        # ── 均線排列 ───────────────────────────────────────
        elif ctype == 'ma_bull_alignment':
            ma5  = safe_float(_ma(5).iloc[-1])
            ma10 = safe_float(_ma(10).iloc[-1])
            ma20 = safe_float(_ma(20).iloc[-1])
            ma60 = safe_float(_ma(60).iloc[-1])
            passed = ma5 > ma10 > ma20 > ma60
            return passed, f'MA5({ma5:.2f})>MA10({ma10:.2f})>MA20({ma20:.2f})>MA60({ma60:.2f})'

        elif ctype == 'ma_bear_alignment':
            ma5  = safe_float(_ma(5).iloc[-1])
            ma10 = safe_float(_ma(10).iloc[-1])
            ma20 = safe_float(_ma(20).iloc[-1])
            ma60 = safe_float(_ma(60).iloc[-1])
            passed = ma5 < ma10 < ma20 < ma60
            return passed, f'MA5({ma5:.2f})<MA10({ma10:.2f})<MA20({ma20:.2f})<MA60({ma60:.2f})'

        elif ctype == 'ma_golden_cross':
            short_p = int(params.get('short_period', 5))
            long_p  = int(params.get('long_period', 20))
            within  = int(params.get('within_days', 5))
            ma_s = _ma(short_p)
            ma_l = _ma(long_p)
            if n < within + 2:
                return False, '資料不足'
            curr_above = safe_float(ma_s.iloc[-1]) > safe_float(ma_l.iloc[-1])
            was_below  = (ma_s.iloc[-(within+1):-1].values < ma_l.iloc[-(within+1):-1].values).any()
            return (curr_above and was_below,
                    f'MA{short_p} 近{within}天突破 MA{long_p}')

        elif ctype == 'ma_death_cross':
            short_p = int(params.get('short_period', 5))
            long_p  = int(params.get('long_period', 20))
            within  = int(params.get('within_days', 5))
            ma_s = _ma(short_p)
            ma_l = _ma(long_p)
            if n < within + 2:
                return False, '資料不足'
            curr_below = safe_float(ma_s.iloc[-1]) < safe_float(ma_l.iloc[-1])
            was_above  = (ma_s.iloc[-(within+1):-1].values > ma_l.iloc[-(within+1):-1].values).any()
            return (curr_below and was_above,
                    f'MA{short_p} 近{within}天跌破 MA{long_p}')

        elif ctype == 'ma_trending_down':
            period     = int(params.get('period', 20))
            trend_days = int(params.get('trend_days', 5))
            ma_series  = _ma(period)
            if n < trend_days + 2:
                return False, '資料不足'
            return (safe_float(last_valid(ma_series)) < safe_float(ma_series.iloc[-trend_days]),
                    f'MA{period} {trend_days}天持續下降')

        # ── 價格形態 ───────────────────────────────────────
        elif ctype == 'price_near_52w_low':
            thr = float(params.get('threshold', 10))
            low52 = safe_float(hist['Low'].rolling(min(252, n)).min().iloc[-1])
            dist = (price - low52) / low52 * 100 if low52 > 0 else 999
            return dist <= thr, f'距52週低 {dist:.1f}% ≤ {thr}%'

        elif ctype == 'price_nd_high':
            days = int(params.get('days', 20))
            peak = safe_float(hist['High'].iloc[-days:].max()) if n >= days else safe_float(hist['High'].max())
            prev_peak = safe_float(hist['High'].iloc[-days-1:-1].max()) if n > days else peak
            passed = price >= prev_peak * 0.995
            return passed, f'股價 {price:.2f} 創近{days}日新高 {prev_peak:.2f}'

        elif ctype == 'consecutive_up':
            days = int(params.get('days', 3))
            if n < days + 1:
                return False, '資料不足'
            recent = close.dropna().iloc[-(days+1):]
            passed = all(recent.iloc[i] > recent.iloc[i-1] for i in range(1, len(recent)))
            return passed, f'連續上漲 {days} 天'

        elif ctype == 'consecutive_down':
            days = int(params.get('days', 3))
            if n < days + 1:
                return False, '資料不足'
            recent = close.dropna().iloc[-(days+1):]
            passed = all(recent.iloc[i] < recent.iloc[i-1] for i in range(1, len(recent)))
            return passed, f'連續下跌 {days} 天'

        elif ctype == 'price_change_nd':
            days = int(params.get('days', 3))
            thr  = float(params.get('threshold', 5))
            if n < days + 1:
                return False, '資料不足'
            base = safe_float(close.dropna().iloc[-days-1])
            chg  = (price / base - 1) * 100 if base > 0 else 0
            return chg >= thr, f'近{days}日累計漲幅 {chg:.2f}% ≥ {thr}%'

        elif ctype == 'price_change_nd_down':
            days = int(params.get('days', 3))
            thr  = float(params.get('threshold', -5))
            if n < days + 1:
                return False, '資料不足'
            base = safe_float(close.dropna().iloc[-days-1])
            chg  = (price / base - 1) * 100 if base > 0 else 0
            return chg <= thr, f'近{days}日累計跌幅 {chg:.2f}% ≤ {thr}%'

        elif ctype == 'price_gap_up':
            if n < 2:
                return False, '資料不足'
            today_open = safe_float(hist['Open'].iloc[-1])
            yest_high  = safe_float(hist['High'].iloc[-2])
            passed = today_open > yest_high
            return passed, f'今日開盤({today_open:.2f}) > 昨日最高({yest_high:.2f})'

        elif ctype == 'high_vol_breakout':
            days = int(params.get('days', 20))
            if n < days + 1:
                return False, '資料不足'
            prev_peak = safe_float(hist['High'].iloc[-days-1:-1].max())
            curr_high = safe_float(hist['High'].iloc[-1])
            avg_vol   = safe_float(vol.rolling(days, min_periods=1).mean().iloc[-1])
            curr_vol  = safe_float(vol.iloc[-1])
            vol_ok    = curr_vol >= avg_vol * 1.3
            price_ok  = curr_high >= prev_peak
            return (price_ok and vol_ok,
                    f'突破{days}日高點{prev_peak:.2f}，量比{curr_vol/avg_vol if avg_vol else 0:.2f}x')

        # ── RSI 穿越 ───────────────────────────────────────
        elif ctype == 'rsi_cross_above':
            period = int(params.get('period', 14))
            thr    = float(params.get('threshold', 50))
            within = int(params.get('within_days', 3))
            rsi_s  = calc_rsi(close, period)
            if n < within + 2:
                return False, '資料不足'
            curr_above = safe_float(rsi_s.iloc[-1]) > thr
            was_below  = (rsi_s.iloc[-(within+1):-1] < thr).any()
            return (curr_above and was_below,
                    f'RSI({period}) 近{within}天上穿 {thr}')

        elif ctype == 'rsi_cross_below':
            period = int(params.get('period', 14))
            thr    = float(params.get('threshold', 70))
            within = int(params.get('within_days', 3))
            rsi_s  = calc_rsi(close, period)
            if n < within + 2:
                return False, '資料不足'
            curr_below = safe_float(rsi_s.iloc[-1]) < thr
            was_above  = (rsi_s.iloc[-(within+1):-1] > thr).any()
            return (curr_below and was_above,
                    f'RSI({period}) 近{within}天下穿 {thr}')

        # ── 布林帶 ─────────────────────────────────────────
        elif ctype == 'bb_squeeze':
            thr = float(params.get('threshold', 5))
            bb_u, bb_m, bb_l = calc_bollinger(close)
            bbu = safe_float(bb_u.iloc[-1])
            bbl = safe_float(bb_l.iloc[-1])
            bbm = safe_float(bb_m.iloc[-1])
            bw  = (bbu - bbl) / bbm * 100 if bbm > 0 else 999
            return bw <= thr, f'布林帶寬 {bw:.2f}% ≤ {thr}%（帶寬收窄）'

        elif ctype == 'bb_breakout_up':
            bb_u, bb_m, bb_l = calc_bollinger(close)
            bbu = safe_float(bb_u.iloc[-1])
            return price >= bbu, f'股價 {price:.2f} ≥ 布林上軌 {bbu:.2f}'

        # ── 成交量 ─────────────────────────────────────────
        elif ctype == 'volume_nd_high':
            days = int(params.get('days', 20))
            if n < days + 1:
                return False, '資料不足'
            prev_max = safe_float(vol.iloc[-days-1:-1].max())
            curr_vol = safe_float(vol.iloc[-1])
            passed   = curr_vol >= prev_max
            return passed, f'成交量 {curr_vol:.0f} 創近{days}日新高 {prev_max:.0f}'

        elif ctype == 'obv_rising':
            trend_days = int(params.get('trend_days', 10))
            if n < trend_days + 2:
                return False, '資料不足'
            daily_chg = close.diff()
            obv = (vol * daily_chg.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))).cumsum()
            passed = safe_float(obv.iloc[-1]) > safe_float(obv.iloc[-trend_days])
            return passed, f'OBV {trend_days}天持續上升'

        # ── 基本面 ─────────────────────────────────────────
        elif ctype == 'pe_below':
            thr = float(params.get('threshold', 20))
            pe  = safe_float(info.get('trailingPE') or info.get('forwardPE') or 0)
            if pe <= 0:
                return False, 'PE 資料不足'
            return pe <= thr, f'PE {pe:.1f} ≤ {thr}'

        elif ctype == 'pe_above':
            thr = float(params.get('threshold', 30))
            pe  = safe_float(info.get('trailingPE') or info.get('forwardPE') or 0)
            if pe <= 0:
                return False, 'PE 資料不足'
            return pe >= thr, f'PE {pe:.1f} ≥ {thr}'

        elif ctype == 'div_yield_above':
            thr = float(params.get('threshold', 3))
            dy  = safe_float(info.get('dividendYield', 0)) * 100
            return dy >= thr, f'股息率 {dy:.2f}% ≥ {thr}%'

        elif ctype == 'market_cap_above':
            thr = float(params.get('threshold', 10)) * 1e9
            mc  = safe_float(info.get('marketCap', 0))
            return mc >= thr, f'市值 {mc/1e9:.1f}B ≥ {params.get("threshold",10)}B'

        elif ctype == 'market_cap_below':
            thr = float(params.get('threshold', 2)) * 1e9
            mc  = safe_float(info.get('marketCap', 0))
            if mc <= 0:
                return False, '市值資料不足'
            return mc <= thr, f'市值 {mc/1e9:.1f}B ≤ {params.get("threshold",2)}B'

        # ── K線型態 ──────────────────────────────────────────────────────
        elif ctype == 'candle_big_red':
            thr = float(params.get('threshold', 3))
            o, c = safe_float(hist['Open'].iloc[-1]), price
            pct = (c / o - 1) * 100 if o > 0 else 0
            return pct >= thr, f'紅K棒漲幅 {pct:.1f}% ≥ {thr}%'

        elif ctype == 'candle_long_lower_wick':
            thr = float(params.get('threshold', 50))
            o = safe_float(hist['Open'].iloc[-1])
            h = safe_float(hist['High'].iloc[-1])
            l = safe_float(hist['Low'].iloc[-1])
            body_lo = min(o, price); rng = h - l
            lower = body_lo - l
            ratio = lower / rng * 100 if rng > 0 else 0
            return ratio >= thr, f'下影線佔比 {ratio:.0f}% ≥ {thr}%'

        elif ctype == 'candle_hammer':
            if n < 1: return False, '資料不足'
            o = safe_float(hist['Open'].iloc[-1])
            h = safe_float(hist['High'].iloc[-1])
            l = safe_float(hist['Low'].iloc[-1])
            body = abs(price - o); rng = h - l
            body_lo = min(o, price); body_hi = max(o, price)
            lower = body_lo - l; upper = h - body_hi
            ok = (rng > 0 and body / rng < 0.35
                  and lower >= 2 * body and upper <= body * 0.5)
            return ok, f'鎚頭型態 下影:{lower:.2f} 實體:{body:.2f}'

        elif ctype == 'candle_inv_hammer':
            if n < 1: return False, '資料不足'
            o = safe_float(hist['Open'].iloc[-1])
            h = safe_float(hist['High'].iloc[-1])
            l = safe_float(hist['Low'].iloc[-1])
            body = abs(price - o); rng = h - l
            body_lo = min(o, price); body_hi = max(o, price)
            upper = h - body_hi; lower = body_lo - l
            ok = (rng > 0 and body / rng < 0.35
                  and upper >= 2 * body and lower <= body * 0.5)
            return ok, f'倒狀槌子 上影:{upper:.2f} 實體:{body:.2f}'

        elif ctype == 'candle_bullish_engulfing':
            if n < 2: return False, '資料不足'
            po = safe_float(hist['Open'].iloc[-2]); pc = safe_float(hist['Close'].iloc[-2])
            co = safe_float(hist['Open'].iloc[-1]); cc = price
            ok = (pc < po and cc > co          # 前陰後陽
                  and co <= pc and cc >= po)   # 今陽包前陰
            return ok, f'多頭吞噬 昨陰收{pc:.2f} 今陽開{co:.2f}收{cc:.2f}'

        elif ctype == 'candle_harami':
            if n < 2: return False, '資料不足'
            po = safe_float(hist['Open'].iloc[-2]); pc = safe_float(hist['Close'].iloc[-2])
            co = safe_float(hist['Open'].iloc[-1]); cc = price
            big_lo = min(po, pc); big_hi = max(po, pc)
            ok = (pc < po                        # 前長陰
                  and cc > co                    # 今陽
                  and co >= big_lo and cc <= big_hi)  # 在前陰範圍內
            return ok, f'多頭母子 昨陰({po:.2f}→{pc:.2f}) 今小陽({co:.2f}→{cc:.2f})'

        elif ctype == 'candle_morning_star':
            if n < 3: return False, '資料不足'
            o1=safe_float(hist['Open'].iloc[-3]); c1=safe_float(hist['Close'].iloc[-3])
            o2=safe_float(hist['Open'].iloc[-2]); c2=safe_float(hist['Close'].iloc[-2])
            o3=safe_float(hist['Open'].iloc[-1]); c3=price
            body1=abs(c1-o1); body2=abs(c2-o2); body3=abs(c3-o3)
            ok = (c1 < o1 and body1 > 0            # 第1根陰
                  and body2 < body1 * 0.5          # 第2根小實體（星）
                  and c3 > o3                      # 第3根陽
                  and c3 > (o1 + c1) / 2)          # 第3根收盤超過第1根中點
            return ok, f'晨星型態 ({c1:.2f},{c2:.2f},{c3:.2f})'

        elif ctype == 'candle_three_soldiers':
            if n < 3: return False, '資料不足'
            rows = [(safe_float(hist['Open'].iloc[-(i+1)]),
                     safe_float(hist['Close'].iloc[-(i+1)])) for i in range(3)][::-1]
            ok = all(c > o for o, c in rows)   # 三根皆陽
            ok = ok and rows[1][1] > rows[0][1] and rows[2][1] > rows[1][1]  # 連續創高
            ok = ok and (rows[1][0] >= rows[0][0] and rows[1][0] <= rows[0][1])  # 開盤在前根實體內
            return ok, f'紅三兵 收盤({rows[0][1]:.2f},{rows[1][1]:.2f},{rows[2][1]:.2f})'

        elif ctype == 'candle_belt_hold':
            if n < 1: return False, '資料不足'
            o = safe_float(hist['Open'].iloc[-1])
            l = safe_float(hist['Low'].iloc[-1])
            ok = (price > o and abs(o - l) / (price - l) < 0.05 if (price - l) > 0 else False)
            return ok, f'多頭執帶 開{o:.2f}=最低 收{price:.2f}'

        elif ctype == 'candle_meeting_line':
            if n < 2: return False, '資料不足'
            pc = safe_float(hist['Close'].iloc[-2])
            po = safe_float(hist['Open'].iloc[-2])
            ok = (pc < po
                  and price > safe_float(hist['Open'].iloc[-1])
                  and abs(price - pc) / pc < 0.01)
            return ok, f'多頭遭遇 昨收{pc:.2f} 今收{price:.2f}'

        # ── 漲跌停、相對強弱 ──────────────────────────────────────────────
        elif ctype == 'limit_up':
            prev_c = safe_float(hist['Close'].dropna().iloc[-2]) if n > 1 else price
            pct = (price / prev_c - 1) * 100 if prev_c > 0 else 0
            limit = float(params.get('limit', 9.5))
            return pct >= limit, f'漲幅 {pct:.1f}% ≥ {limit}%（漲停）'

        elif ctype == 'limit_down':
            prev_c = safe_float(hist['Close'].dropna().iloc[-2]) if n > 1 else price
            pct = (price / prev_c - 1) * 100 if prev_c > 0 else 0
            limit = float(params.get('limit', -9.5))
            return pct <= limit, f'跌幅 {pct:.1f}% ≤ {limit}%（跌停）'

        elif ctype == 'outperform_index':
            # 股票N日漲幅 > 大盤N日漲幅
            days = int(params.get('days', 5))
            if n < days + 1: return False, '資料不足'
            stk_ret = (price / safe_float(close.iloc[-(days+1)]) - 1) * 100 if safe_float(close.iloc[-(days+1)]) > 0 else 0
            idx_ret = safe_float(params.get('index_return', 0))  # fallback: just check positive
            # 無大盤資料時改為：近N日漲幅 > 0
            return stk_ret > 0, f'{days}日漲幅 {stk_ret:.1f}%（優於持平）'

        elif ctype == 'close_near_high':
            # 收在最高（收盤 = 當日最高）
            h = safe_float(hist['High'].iloc[-1])
            ok = h > 0 and (h - price) / h < 0.01
            return ok, f'收盤{price:.2f} 接近最高{h:.2f}'

        # ── MTM 動能指標 ──────────────────────────────────────────────────
        elif ctype == 'mtm_cross_above':
            period = int(params.get('period', 6))
            if n < period + 2: return False, '資料不足'
            mtm_now  = price - safe_float(close.iloc[-(period+1)])
            mtm_prev = safe_float(close.iloc[-2]) - safe_float(close.iloc[-(period+2)]) if n > period+1 else 0
            ok = mtm_prev < 0 <= mtm_now
            return ok, f'MTM({period}) 由負({mtm_prev:.2f})轉正({mtm_now:.2f})'

        elif ctype == 'mtm_positive':
            period = int(params.get('period', 6))
            if n < period + 1: return False, '資料不足'
            mtm = price - safe_float(close.iloc[-(period+1)])
            return mtm > 0, f'MTM({period}) = {mtm:.2f} > 0'

        # ── 週線條件（需 weekly hist）─────────────────────────────────────
        elif ctype == 'weekly_price_above_ma':
            wh = (extra or {}).get('weekly')
            if wh is None or len(wh) < 6: return False, '週線資料不足'
            period = int(params.get('period', 5))
            wp = safe_float(wh['Close'].iloc[-1])
            wma = safe_float(wh['Close'].rolling(min(period, len(wh))).mean().iloc[-1])
            return wp > wma, f'週K {wp:.2f} > {period}週MA {wma:.2f}'

        elif ctype == 'weekly_price_below_ma':
            wh = (extra or {}).get('weekly')
            if wh is None or len(wh) < 6: return False, '週線資料不足'
            period = int(params.get('period', 5))
            wp = safe_float(wh['Close'].iloc[-1])
            wma = safe_float(wh['Close'].rolling(min(period, len(wh))).mean().iloc[-1])
            return wp < wma, f'週K {wp:.2f} < {period}週MA {wma:.2f}'

        elif ctype == 'weekly_ma_trending_up':
            wh = (extra or {}).get('weekly')
            if wh is None or len(wh) < 8: return False, '週線資料不足'
            period = int(params.get('period', 5))
            wma = wh['Close'].rolling(min(period, len(wh))).mean()
            ok = safe_float(wma.iloc[-1]) > safe_float(wma.iloc[-3])
            return ok, f'{period}週MA翻揚 {safe_float(wma.iloc[-3]):.2f}→{safe_float(wma.iloc[-1]):.2f}'

        elif ctype == 'weekly_ma_trending_down':
            wh = (extra or {}).get('weekly')
            if wh is None or len(wh) < 8: return False, '週線資料不足'
            period = int(params.get('period', 5))
            wma = wh['Close'].rolling(min(period, len(wh))).mean()
            ok = safe_float(wma.iloc[-1]) < safe_float(wma.iloc[-3])
            return ok, f'{period}週MA翻黑 {safe_float(wma.iloc[-3]):.2f}→{safe_float(wma.iloc[-1]):.2f}'

        elif ctype == 'bb_oversold':
            # 布林通道超賣（收盤低於下軌）
            period = int(params.get('period', 20))
            std_dev = float(params.get('std_dev', 2))
            bbu, bbm, bbl = calc_bollinger(close, period, std_dev)
            bbl_v = safe_float(bbl.iloc[-1])
            return price < bbl_v, f'收盤{price:.2f} < 布林下軌{bbl_v:.2f}'

        elif ctype == 'vol_up_candle':
            # 上漲成交量創N日新高
            days = int(params.get('days', 5))
            if n < days + 1: return False, '資料不足'
            today_up = price >= safe_float(close.iloc[-2]) if n > 1 else True
            curr_vol = safe_float(vol.iloc[-1])
            max_vol  = safe_float(vol.iloc[-(days+1):-1].max())
            return today_up and curr_vol > max_vol, f'上漲量{curr_vol:.0f}>{days}日最高{max_vol:.0f}'

        elif ctype == 'vol_down_candle':
            # 下跌成交量創N日新高（賣壓警示）
            days = int(params.get('days', 5))
            if n < days + 1: return False, '資料不足'
            today_dn = price < safe_float(close.iloc[-2]) if n > 1 else False
            curr_vol = safe_float(vol.iloc[-1])
            max_vol  = safe_float(vol.iloc[-(days+1):-1].max())
            return today_dn and curr_vol > max_vol, f'下跌量{curr_vol:.0f}>{days}日最高{max_vol:.0f}'

        elif ctype == 'candle_doji':
            o = safe_float(hist['Open'].iloc[-1])
            h = safe_float(hist['High'].iloc[-1]); l = safe_float(hist['Low'].iloc[-1])
            body = abs(price - o); rng = h - l
            ok = rng > 0 and body / rng < 0.1
            return ok, f'十字星 實體{body:.2f} 全幅{rng:.2f}'

        elif ctype == 'candle_shooting_star':
            o = safe_float(hist['Open'].iloc[-1])
            h = safe_float(hist['High'].iloc[-1]); l = safe_float(hist['Low'].iloc[-1])
            body = abs(price - o); body_hi = max(price, o); body_lo = min(price, o)
            upper = h - body_hi; lower = body_lo - l; rng = h - l
            ok = (rng > 0 and body / rng < 0.3
                  and upper >= 2 * body and lower <= body * 0.3
                  and price < o)   # 陰線
            return ok, f'射擊之星 上影{upper:.2f} 實體{body:.2f}'

        elif ctype == 'candle_hanging_man':
            o = safe_float(hist['Open'].iloc[-1])
            h = safe_float(hist['High'].iloc[-1]); l = safe_float(hist['Low'].iloc[-1])
            body = abs(price - o); body_lo = min(price, o); body_hi = max(price, o)
            lower = body_lo - l; upper = h - body_hi; rng = h - l
            # 上吊線：在高位出現的鎚頭形狀（需配合前高）
            ok = (rng > 0 and body / rng < 0.35
                  and lower >= 2 * body and upper <= body * 0.5)
            return ok, f'上吊線 下影{lower:.2f} 實體{body:.2f}'

        elif ctype == 'candle_bearish_engulfing':
            if n < 2: return False, '資料不足'
            po = safe_float(hist['Open'].iloc[-2]); pc = safe_float(hist['Close'].iloc[-2])
            co = safe_float(hist['Open'].iloc[-1]); cc = price
            ok = (pc > po and cc < co and co >= po and cc <= pc)
            return ok, f'空頭吞噬 昨陽收{pc:.2f} 今陰開{co:.2f}收{cc:.2f}'

        elif ctype == 'candle_evening_star':
            if n < 3: return False, '資料不足'
            o1=safe_float(hist['Open'].iloc[-3]); c1=safe_float(hist['Close'].iloc[-3])
            o2=safe_float(hist['Open'].iloc[-2]); c2=safe_float(hist['Close'].iloc[-2])
            o3=safe_float(hist['Open'].iloc[-1]); c3=price
            body1=abs(c1-o1); body2=abs(c2-o2)
            ok = (c1 > o1 and body2 < body1 * 0.5
                  and c3 < o3 and c3 < (o1 + c1) / 2)
            return ok, f'夜星型態 ({c1:.2f},{c2:.2f},{c3:.2f})'

        elif ctype == 'candle_three_crows':
            if n < 3: return False, '資料不足'
            rows = [(safe_float(hist['Open'].iloc[-(i+1)]),
                     safe_float(hist['Close'].iloc[-(i+1)])) for i in range(3)][::-1]
            ok = all(c < o for o, c in rows)
            ok = ok and rows[1][1] < rows[0][1] and rows[2][1] < rows[1][1]
            return ok, f'黑三兵 收盤({rows[0][1]:.2f},{rows[1][1]:.2f},{rows[2][1]:.2f})'

        # ── 技術指標 ─────────────────────────────────────────────────────
        elif ctype == 'william_r_oversold':
            period = int(params.get('period', 14))
            thr    = float(params.get('threshold', -80))
            wr = calc_william_r(high, low, close, period)
            wv = safe_float(wr.iloc[-1])
            return wv <= thr, f'WR({period}) = {wv:.1f} ≤ {thr}'

        elif ctype == 'william_r_cross_above':
            period = int(params.get('period', 14))
            thr    = float(params.get('threshold', -80))
            within = int(params.get('within_days', 3))
            wr = calc_william_r(high, low, close, period)
            for i in range(-within, 0):
                try:
                    if safe_float(wr.iloc[i-1]) < thr <= safe_float(wr.iloc[i]):
                        return True, f'WR({period}) 穿越{thr} 現值{safe_float(wr.iloc[-1]):.1f}'
                except Exception: pass
            return False, f'WR({period}) = {safe_float(wr.iloc[-1]):.1f} 未穿越{thr}'

        elif ctype == 'cci_oversold':
            period = int(params.get('period', 20))
            thr    = float(params.get('threshold', -100))
            cc_s   = calc_cci(high, low, close, period)
            cv     = safe_float(cc_s.iloc[-1])
            return cv <= thr, f'CCI({period}) = {cv:.0f} ≤ {thr}'

        elif ctype == 'cci_cross_above':
            period = int(params.get('period', 20))
            thr    = float(params.get('threshold', -100))
            within = int(params.get('within_days', 3))
            cc_s   = calc_cci(high, low, close, period)
            for i in range(-within, 0):
                try:
                    if safe_float(cc_s.iloc[i-1]) < thr <= safe_float(cc_s.iloc[i]):
                        return True, f'CCI({period}) 穿越{thr} 現值{safe_float(cc_s.iloc[-1]):.0f}'
                except Exception: pass
            return False, f'CCI({period}) = {safe_float(cc_s.iloc[-1]):.0f} 未穿越{thr}'

        elif ctype == 'adx_strong_trend':
            period = int(params.get('period', 14))
            thr    = float(params.get('threshold', 25))
            adx_s, dip, dim = calc_adx(high, low, close, period)
            av = safe_float(adx_s.iloc[-1])
            bull = safe_float(dip.iloc[-1]) > safe_float(dim.iloc[-1])
            return av >= thr and bull, f'ADX({period}) = {av:.1f} ≥ {thr} ({"多" if bull else "空"}頭)'

        elif ctype == 'bias_low':
            period = int(params.get('period', 20))
            thr    = float(params.get('threshold', -5))
            bv     = safe_float(calc_bias(close, period).iloc[-1])
            return bv <= thr, f'乖離率({period}) = {bv:.1f}% ≤ {thr}%'

        elif ctype == 'bias_high':
            period = int(params.get('period', 20))
            thr    = float(params.get('threshold', 10))
            bv     = safe_float(calc_bias(close, period).iloc[-1])
            return bv >= thr, f'乖離率({period}) = {bv:.1f}% ≥ {thr}%'

        elif ctype == 'ma_deduction_up':
            # 月線扣抵向上：明日將離開均線窗口的舊收盤 < 現價 → 均線明日翻揚
            period = int(params.get('period', 20))
            if n <= period:
                return False, f'資料不足{period}日'
            deduct = safe_float(close.iloc[n - 1 - period])
            price_now = safe_float(close.iloc[-1])
            ok = price_now > deduct
            return ok, f'{period}日扣抵價 {deduct:.2f}，現價 {price_now:.2f} → 均線{"上揚" if ok else "下彎"}'

        elif ctype == 'vol_5_above_20':
            # 量能結構轉強：5日均量站上20日均量
            v5  = safe_float(vol.rolling(min(5, n), min_periods=1).mean().iloc[-1])
            v20 = safe_float(vol.rolling(min(20, n), min_periods=1).mean().iloc[-1])
            return v5 > v20, f'5日均量 {v5/1000:.0f}張 {">" if v5 > v20 else "≤"} 20日均量 {v20/1000:.0f}張'

        elif ctype == 'kd_low_golden_cross':
            # KD 低檔(<門檻)黃金交叉，比一般金叉更精準的起漲訊號
            kn  = int(params.get('kd_n', 9))
            thr = float(params.get('threshold', 30))
            within = int(params.get('within_days', 3))
            k, d_ = calc_kd(high, low, close, kn, 3, 3)
            for i in range(-within, 0):
                try:
                    if k.iloc[i-1] < d_.iloc[i-1] and k.iloc[i] > d_.iloc[i] and k.iloc[i] < thr:
                        return True, f'KD 低檔金叉 K={safe_float(k.iloc[-1]):.1f}（<{thr}）'
                except Exception:
                    pass
            return False, f'近{within}日無低檔金叉 K={safe_float(k.iloc[-1]):.1f}'

        elif ctype == 'pullback_hold_ma':
            # 回測均線不破：現價在均線上方但乖離很小（回後守穩，續攻機率高）
            period = int(params.get('period', 20))
            within = float(params.get('within_pct', 3))
            ma = safe_float(close.rolling(min(period, n), min_periods=1).mean().iloc[-1])
            price_now = safe_float(close.iloc[-1])
            if ma <= 0:
                return False, '均線無效'
            gap = (price_now - ma) / ma * 100
            ok = 0 <= gap <= within
            return ok, f'現價距{period}日線 {gap:+.1f}%（0~{within}% 視為回測守穩）'

        elif ctype == 'psy_low':
            period = int(params.get('period', 12))
            thr    = float(params.get('threshold', 25))
            pv     = safe_float(calc_psy(close, period).iloc[-1])
            return pv <= thr, f'PSY({period}) = {pv:.1f}% ≤ {thr}%'

        elif ctype == 'psy_high':
            period = int(params.get('period', 12))
            thr    = float(params.get('threshold', 75))
            pv     = safe_float(calc_psy(close, period).iloc[-1])
            return pv >= thr, f'PSY({period}) = {pv:.1f}% ≥ {thr}%'

        elif ctype == 'atr_expand':
            period = int(params.get('period', 14))
            ratio  = float(params.get('ratio', 1.5))
            atr_s  = calc_atr(high, low, close, period)
            if n < period + 5: return False, '資料不足'
            curr = safe_float(atr_s.iloc[-1])
            prev = safe_float(atr_s.iloc[-period])
            ok = curr >= prev * ratio if prev > 0 else False
            return ok, f'ATR擴張 {prev:.2f}→{curr:.2f} ({curr/prev:.1f}x)' if prev > 0 else 'ATR資料不足'

        # ── 量價條件 ──────────────────────────────────────────────────────
        elif ctype == 'vol_price_divergence_up':
            # 價漲量縮（背離警示）
            days = int(params.get('days', 3))
            if n < days + 1: return False, '資料不足'
            price_up = close.iloc[-1] > close.iloc[-days-1]
            vol_down = vol.iloc[-days:].mean() < vol.iloc[-days*2:-days].mean() * 0.8 if n >= days*2 else False
            return price_up and vol_down, f'價漲量縮(近{days}日均量下降)'

        elif ctype == 'vol_price_divergence_down':
            # 價跌量縮（打底訊號）
            days = int(params.get('days', 3))
            if n < days + 1: return False, '資料不足'
            price_dn = close.iloc[-1] < close.iloc[-days-1]
            vol_down = vol.iloc[-days:].mean() < vol.iloc[-days*2:-days].mean() * 0.8 if n >= days*2 else False
            return price_dn and vol_down, f'價跌量縮（打底訊號，近{days}日）'

        elif ctype == 'big_vol_red':
            # 大量紅K：量比>N倍 + 今日上漲>M%
            ratio_thr = float(params.get('ratio', 1.5))
            pct_thr   = float(params.get('pct', 2))
            avg_v = safe_float(vol.rolling(min(20,n), min_periods=1).mean().iloc[-1])
            curr_v = safe_float(vol.iloc[-1])
            vr = curr_v / avg_v if avg_v > 0 else 0
            o  = safe_float(hist['Open'].iloc[-1])
            pp = (price / o - 1) * 100 if o > 0 else 0
            return vr >= ratio_thr and pp >= pct_thr, f'大量紅K 量比{vr:.1f}x 漲{pp:.1f}%'

        elif ctype == 'price_consolidation_break':
            # N日盤整後放量突破
            days = int(params.get('days', 10))
            ratio = float(params.get('ratio', 1.5))
            if n < days + 1: return False, '資料不足'
            period_high = safe_float(high.iloc[-(days+1):-1].max())
            period_low  = safe_float(low.iloc[-(days+1):-1].min())
            range_pct   = (period_high - period_low) / period_low * 100 if period_low > 0 else 0
            avg_v = safe_float(vol.iloc[-(days+1):-1].mean())
            curr_v = safe_float(vol.iloc[-1])
            breakout = price > period_high and (curr_v >= avg_v * ratio if avg_v > 0 else False)
            return breakout, f'{days}日盤整({range_pct:.1f}%)後放量突破 {period_high:.2f}'

        elif ctype == 'ma_convergence':
            # 均線糾結：MA5/MA20/MA60 相互距離 < N%
            thr = float(params.get('threshold', 3))
            if n < 60: return False, '資料不足'
            m5  = safe_float(close.rolling(5).mean().iloc[-1])
            m20 = safe_float(close.rolling(20).mean().iloc[-1])
            m60 = safe_float(close.rolling(60).mean().iloc[-1])
            spread = (max(m5,m20,m60) - min(m5,m20,m60)) / min(m5,m20,m60) * 100 if min(m5,m20,m60) > 0 else 99
            return spread <= thr, f'均線糾結 MA5={m5:.2f} MA20={m20:.2f} MA60={m60:.2f} 差距{spread:.1f}%'

        elif ctype == 'monthly_price_above_ma':
            # 月線站上N月均線
            mh = (extra or {}).get('monthly')
            if mh is None or len(mh) < 6: return False, '月線資料不足'
            period = int(params.get('period', 6))
            mp = safe_float(mh['Close'].iloc[-1])
            ma = safe_float(mh['Close'].rolling(min(period, len(mh))).mean().iloc[-1])
            return mp > ma, f'月K收盤{mp:.2f} > {period}月MA {ma:.2f}'

        # ── 三大法人（台股限定）────────────────────────────────────────────
        elif ctype in ('inst_foreign_buy', 'inst_foreign_sell', 'inst_trust_buy',
                       'inst_trust_sell', 'inst_dealer_buy', 'inst_3_buy',
                       'inst_total_above', 'inst_foreign_dominant'):
            it = (extra or {}).get('inst')
            if it is None:
                return False, '非台股或無法人資料'
            fn = it.get('foreign_net', 0)
            tn = it.get('trust_net', 0)
            dn = it.get('dealer_net', 0)
            tot = it.get('total_net', 0)

            if ctype == 'inst_foreign_buy':
                return fn > 0, f'外資買超 {fn:,.0f}股'
            elif ctype == 'inst_foreign_sell':
                return fn < 0, f'外資賣超 {abs(fn):,.0f}股'
            elif ctype == 'inst_trust_buy':
                return tn > 0, f'投信買超 {tn:,.0f}股'
            elif ctype == 'inst_trust_sell':
                return tn < 0, f'投信賣超 {abs(tn):,.0f}股'
            elif ctype == 'inst_dealer_buy':
                return dn > 0, f'自營商買超 {dn:,.0f}股'
            elif ctype == 'inst_3_buy':
                return tot > 0, f'三大法人合計買超 {tot:,.0f}股'
            elif ctype == 'inst_total_above':
                thr = float(params.get('threshold', 1000)) * 1000
                return tot >= thr, f'三大法人合計{tot/1000:.0f}千股 ≥ {params.get("threshold",1000)}千股'
            elif ctype == 'inst_foreign_dominant':
                # 外資主導（外資買超佔三大法人 > 80%）
                ok = tot > 0 and fn > 0 and fn / tot >= 0.8
                return ok, f'外資主導 外資{fn:,.0f} 合計{tot:,.0f}'

        # ── 三大法人「連續N日」買超（多日序列）─────────────────────────────
        elif ctype in ('inst_foreign_buy_ndays', 'inst_trust_buy_ndays',
                       'inst_3_buy_ndays', 'inst_net_sum_above', 'inst_foreign_sell_ndays'):
            ih = (extra or {}).get('inst_hist')
            if ih is None:
                return False, '非台股或無多日法人資料'
            days = int(params.get('days', 3))
            fseq = ih.get('foreign', [])[:days]
            tseq = ih.get('trust',   [])[:days]
            sseq = ih.get('total',   [])[:days]

            if ctype == 'inst_foreign_buy_ndays':
                if len(fseq) < days: return False, f'外資資料不足{days}日'
                ok = all(v > 0 for v in fseq)
                return ok, f'外資近{days}日{"連續買超" if ok else "未連續買超"} {[round(v) for v in fseq]}'
            elif ctype == 'inst_foreign_sell_ndays':
                if len(fseq) < days: return False, f'外資資料不足{days}日'
                ok = all(v < 0 for v in fseq)
                return ok, f'外資近{days}日{"連續賣超" if ok else "未連續賣超"}'
            elif ctype == 'inst_trust_buy_ndays':
                if len(tseq) < days: return False, f'投信資料不足{days}日'
                ok = all(v > 0 for v in tseq)
                return ok, f'投信近{days}日{"連續買超" if ok else "未連續買超"} {[round(v) for v in tseq]}'
            elif ctype == 'inst_3_buy_ndays':
                if len(sseq) < days: return False, f'法人資料不足{days}日'
                ok = all(v > 0 for v in sseq)
                return ok, f'三大法人近{days}日{"連續買超" if ok else "未連續買超"}'
            elif ctype == 'inst_net_sum_above':
                thr = float(params.get('threshold', 5000)) * 1000
                ssum = sum(sseq)
                return ssum >= thr, f'近{days}日累計買超 {ssum/1000:.0f}千股 {"≥" if ssum>=thr else "<"} {params.get("threshold",5000)}千股'

        # ── 多週期指標 ────────────────────────────────────────────────────
        elif ctype in ('weekly_kd_golden_cross', 'weekly_macd_golden_cross',
                       'weekly_rsi_cross_above', 'monthly_kd_oversold',
                       'monthly_macd_golden_cross'):
            wh = (extra or {}).get('weekly')
            mh = (extra or {}).get('monthly')
            if ctype == 'weekly_kd_golden_cross':
                if wh is None or len(wh) < 15: return False, '週線資料不足'
                kn = int(params.get('kd_n', 9))
                k, d_ = calc_kd(wh['High'], wh['Low'], wh['Close'], kn, 3, 3)
                within = int(params.get('within_days', 3))
                for i in range(-within, 0):
                    try:
                        if k.iloc[i-1] < d_.iloc[i-1] and k.iloc[i] > d_.iloc[i]:
                            return True, f'週KD({kn}) 金叉 K={safe_float(k.iloc[-1]):.1f}'
                    except Exception: pass
                return False, f'週KD({kn}) 無金叉 K={safe_float(k.iloc[-1]):.1f}'

            elif ctype == 'weekly_macd_golden_cross':
                if wh is None or len(wh) < 30: return False, '週線資料不足'
                within = int(params.get('within_days', 3))
                wm, ws, _ = calc_macd(wh['Close'])
                for i in range(-within, 0):
                    try:
                        if wm.iloc[i-1] < ws.iloc[i-1] and wm.iloc[i] > ws.iloc[i]:
                            return True, f'週MACD金叉 DIF={safe_float(wm.iloc[-1]):.2f}'
                    except Exception: pass
                return False, f'週MACD無金叉 DIF={safe_float(wm.iloc[-1]):.2f}'

            elif ctype == 'weekly_rsi_cross_above':
                if wh is None or len(wh) < 20: return False, '週線資料不足'
                thr = float(params.get('threshold', 50))
                within = int(params.get('within_days', 2))
                wr = calc_rsi(wh['Close'])
                for i in range(-within, 0):
                    try:
                        if safe_float(wr.iloc[i-1]) < thr <= safe_float(wr.iloc[i]):
                            return True, f'週RSI穿越{thr} RSI={safe_float(wr.iloc[-1]):.1f}'
                    except Exception: pass
                return False, f'週RSI={safe_float(wr.iloc[-1]):.1f} 未穿越{thr}'

            elif ctype == 'monthly_kd_oversold':
                if mh is None or len(mh) < 10: return False, '月線資料不足'
                thr = float(params.get('threshold', 20))
                k, _ = calc_kd(mh['High'], mh['Low'], mh['Close'], 9, 3, 3)
                kv = safe_float(k.iloc[-1])
                return kv < thr, f'月KD K={kv:.1f} {"低檔鈍化" if kv < thr else f"> {thr}"}'

            elif ctype == 'monthly_macd_golden_cross':
                if mh is None or len(mh) < 12: return False, '月線資料不足'
                mm, ms, _ = calc_macd(mh['Close'])
                ok = safe_float(mm.iloc[-1]) > safe_float(ms.iloc[-1]) and safe_float(mm.iloc[-2]) <= safe_float(ms.iloc[-2])
                return ok, f'月MACD金叉 DIF={safe_float(mm.iloc[-1]):.2f}'

        # ── 融資融券（台股限定）──────────────────────────────────────────
        elif ctype in ('margin_increase', 'margin_decrease', 'short_decrease',
                       'short_increase', 'high_short_ratio', 'margin_continuous_up'):
            mg = (extra or {}).get('margin')
            if mg is None:
                return False, '非台股或無融資券資料'
            mt = mg.get('margin_today', 0); mp = mg.get('margin_prev', 0)
            st = mg.get('short_today',  0); sp = mg.get('short_prev',  0)

            if ctype == 'margin_increase':
                chg = mt - mp
                return chg > 0, f'融資 {mp:.0f}→{mt:.0f} 增{chg:+.0f}張'

            elif ctype == 'margin_decrease':
                chg = mt - mp
                return chg < 0, f'融資 {mp:.0f}→{mt:.0f} 減{abs(chg):.0f}張'

            elif ctype == 'short_decrease':
                chg = st - sp
                return chg < 0, f'融券 {sp:.0f}→{st:.0f} 減{abs(chg):.0f}張（回補）'

            elif ctype == 'short_increase':
                chg = st - sp
                return chg > 0, f'融券 {sp:.0f}→{st:.0f} 增{chg:+.0f}張'

            elif ctype == 'high_short_ratio':
                thr = float(params.get('threshold', 30))
                ratio = st / mt * 100 if mt > 0 else 0
                return ratio >= thr, f'券資比 {ratio:.1f}% ≥ {thr}%'

            elif ctype == 'margin_continuous_up':
                return mt > mp > 0, f'融資連升 {mp:.0f}→{mt:.0f}張'

        # ── 借券賣出餘額（台股限定）──────────────────────────────────────
        elif ctype in ('lending_decrease', 'lending_increase'):
            ld = (extra or {}).get('lending')
            if ld is None:
                return False, '非台股或無借券資料'
            cur  = ld.get('lending_balance', 0)
            prev = ld.get('lending_balance_prev', 0)
            chg  = cur - prev
            if ctype == 'lending_decrease':
                return chg < 0, f'借券餘額 {prev:,.0f}→{cur:,.0f} 減{abs(chg):,.0f}（空方回補）'
            return chg > 0, f'借券餘額 {prev:,.0f}→{cur:,.0f} 增{chg:+,.0f}（空方加碼）'

        # ── 外資持股比例（台股限定）──────────────────────────────────────
        elif ctype == 'foreign_holding_above':
            fh = (extra or {}).get('holding')
            if fh is None:
                return False, '非台股或無外資持股資料'
            thr = float(params.get('threshold', 10))
            ratio = fh.get('foreign_ratio', 0)
            return ratio >= thr, f'外資持股 {ratio:.1f}% ≥ {thr}%'

        # ── 當沖比（台股限定）────────────────────────────────────────────
        elif ctype == 'day_trade_ratio_above':
            dt = (extra or {}).get('daytrade')
            if dt is None:
                return False, '非台股或無當沖資料'
            thr = float(params.get('threshold', 50))
            dvol = dt.get('volume', 0)
            try:
                volmap = {i.strftime('%Y-%m-%d'): float(v) for i, v in vol.items()}
                tot = volmap.get(dt.get('date', ''), float(vol.iloc[-1]))
            except Exception:
                tot = safe_float(vol.iloc[-1])
            ratio = dvol / tot * 100 if tot > 0 else 0
            return ratio >= thr, f'當沖比 {ratio:.1f}% ≥ {thr}%'

        # ── 5 日均量大於 N 張 ─────────────────────────────────────────────
        elif ctype == 'vol5_avg_above':
            thr_lots = float(params.get('threshold', 1000))   # 單位：張
            avg5_lots = safe_float(vol.rolling(min(5, n), min_periods=1).mean().iloc[-1]) / 1000
            return avg5_lots >= thr_lots, f'5日均量 {avg5_lots:,.0f} 張 ≥ {thr_lots:,.0f} 張'

        # ── 成交量創 5 日新高 / 新低 ───────────────────────────────────────
        elif ctype == 'volume_5d_high':
            d = int(params.get('days', 5))
            cur = safe_float(vol.iloc[-1])
            hi  = safe_float(vol.iloc[-min(d, n):].max())
            ok  = cur >= hi and cur > 0
            return ok, f'量 {cur/1000:,.0f} 張{"創" if ok else "未創"}近{d}日新高'

        elif ctype == 'volume_5d_low':
            d = int(params.get('days', 5))
            cur = safe_float(vol.iloc[-1])
            lo  = safe_float(vol.iloc[-min(d, n):].min())
            ok  = cur <= lo and cur > 0
            return ok, f'量 {cur/1000:,.0f} 張{"創" if ok else "未創"}近{d}日新低'

    except Exception as e:
        return False, f'計算錯誤: {str(e)[:40]}'

    return False, f'未知條件類型: {ctype}'


_WEEKLY_TYPES  = {'weekly_kd_golden_cross','weekly_macd_golden_cross',
                  'weekly_rsi_cross_above','monthly_macd_golden_cross',
                  'weekly_price_above_ma','weekly_price_below_ma',
                  'weekly_ma_trending_up','weekly_ma_trending_down'}
_MONTHLY_TYPES = {'monthly_kd_oversold','monthly_macd_golden_cross','monthly_price_above_ma'}
_MARGIN_TYPES  = {'margin_increase','margin_decrease','short_decrease',
                  'short_increase','high_short_ratio','margin_continuous_up'}
_INST_TYPES    = {'inst_foreign_buy','inst_foreign_sell','inst_trust_buy','inst_trust_sell',
                  'inst_dealer_buy','inst_3_buy','inst_total_above','inst_foreign_dominant'}
_INST_HIST_TYPES = {'inst_foreign_buy_ndays','inst_trust_buy_ndays','inst_3_buy_ndays',
                    'inst_net_sum_above','inst_foreign_sell_ndays'}
_LENDING_TYPES   = {'lending_decrease','lending_increase'}
_HOLDING_TYPES   = {'foreign_holding_above'}
_DAYTRADE_TYPES  = {'day_trade_ratio_above'}

def _scan_ticker(ticker, conditions, is_tw, period='1y', interval='1d'):
    """Scan a single ticker and return result dict or None."""
    try:
        stock = yf.Ticker(ticker)
        info  = stock.info
        if interval == '1h':
            hist = stock.history(period='60d', interval='1h')
        else:
            hist = stock.history(period=period)
        if hist.empty or len(hist) < 20:
            return None
        price = last_valid(hist['Close'])
        if price <= 0:
            return None
        prev      = safe_float(hist['Close'].dropna().iloc[-2]) if len(hist['Close'].dropna()) > 1 else price
        chg_pct   = (price / prev - 1) * 100 if prev else 0
        en_name   = (info.get('shortName') or info.get('longName') or ticker)[:30]
        name      = tw_cn_name(ticker, en_name) if is_tw else en_name

        # ── 按需抓取額外資料 ──
        ctypes = {c.get('type','') for c in conditions}
        extra  = {}
        if ctypes & _WEEKLY_TYPES:
            try:
                wh = stock.history(period='2y', interval='1wk')
                if not wh.empty: extra['weekly'] = wh
            except Exception: pass
        if ctypes & _MONTHLY_TYPES:
            try:
                mh = stock.history(period='5y', interval='1mo')
                if not mh.empty: extra['monthly'] = mh
            except Exception: pass
        if ctypes & _MARGIN_TYPES and is_tw:
            code = ticker.replace('.TW','').replace('.TWO','')
            mg = _get_tw_margin(code)
            if mg: extra['margin'] = mg
        if ctypes & _INST_TYPES and is_tw:
            code = ticker.replace('.TW','').replace('.TWO','')
            it = _get_tw_inst(code)
            if it: extra['inst'] = it
        if ctypes & _INST_HIST_TYPES and is_tw:
            code = ticker.replace('.TW','').replace('.TWO','')
            ih = _get_tw_inst_hist(code)
            if ih: extra['inst_hist'] = ih
        if ctypes & _LENDING_TYPES and is_tw:
            ld = _get_tw_lending(ticker.replace('.TW','').replace('.TWO',''))
            if ld: extra['lending'] = ld
        if ctypes & _HOLDING_TYPES and is_tw:
            fh = _get_tw_foreign_holding(ticker.replace('.TW','').replace('.TWO',''))
            if fh: extra['holding'] = fh
        if ctypes & _DAYTRADE_TYPES and is_tw:
            dt = _get_tw_daytrade(ticker.replace('.TW','').replace('.TWO',''))
            if dt: extra['daytrade'] = dt

        cond_results = []
        all_passed   = True
        for cond in conditions:
            passed, detail = _eval_condition(hist, info, cond, extra)
            passed = bool(passed)
            cond_results.append({'label': cond.get('label', cond['type']),
                                 'passed': passed, 'detail': detail})
            if not passed:
                all_passed = False

        if not all_passed:
            return None

        n      = len(hist['Close'])
        close  = hist['Close']
        ma5    = safe_float(close.rolling(min(5,  n), min_periods=1).mean().iloc[-1])
        ma10   = safe_float(close.rolling(min(10, n), min_periods=1).mean().iloc[-1])
        ma20   = safe_float(close.rolling(min(20, n), min_periods=1).mean().iloc[-1])
        ma60   = safe_float(close.rolling(min(60, n), min_periods=1).mean().iloc[-1])
        rsi    = safe_float(calc_rsi(close).iloc[-1])
        macd_s, sig_s, _ = calc_macd(close)
        avg_vol   = safe_float(hist['Volume'].rolling(min(20,n), min_periods=1).mean().iloc[-1])
        curr_vol  = last_valid(hist['Volume'])
        vol_ratio = curr_vol / avg_vol if avg_vol > 0 else 1.0
        inst_pct  = round(safe_float(info.get('heldPercentInstitutions', 0)) * 100, 1)
        div_yield = round(safe_float(info.get('dividendYield', 0)), 2)
        display = ticker.replace('.TW','').replace('.TWO','') if is_tw else ticker

        return {
            'ticker':    ticker,
            'display':   display,
            'name':      name,
            'price':     round(price, 2),
            'changePct': round(chg_pct, 2),
            'ma5':  round(ma5,  2), 'ma10': round(ma10, 2),
            'ma20': round(ma20, 2), 'ma60': round(ma60, 2),
            'rsi':       round(rsi, 1),
            'macdBull':  safe_float(macd_s.iloc[-1]) > safe_float(sig_s.iloc[-1]),
            'volRatio':  round(vol_ratio, 2),
            'instPct':   inst_pct,
            'divYield':  round(div_yield, 2),
            'isTw':      is_tw,
            'conditions': cond_results,
        }
    except Exception:
        return None


@app.route('/screener')
def screener_page():
    from flask import make_response
    resp = make_response(render_template('screener.html'))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    return resp


@app.route('/api/screener/universe')
def screener_universe():
    return jsonify({'tw': TW_SCREENER_UNIVERSE, 'us': US_SCREENER_UNIVERSE})


@app.route('/api/screener/run', methods=['POST'])
def screener_run():
    data       = request.json or {}
    tickers_in = data.get('tickers', [])
    conditions = data.get('conditions', [])
    is_tw      = data.get('isTw', True)
    period     = data.get('period', '1y')
    interval   = data.get('interval', '1d')

    # Normalize tickers
    if is_tw:
        tickers = [tw_normalize(t.strip()) for t in tickers_in if t.strip()]
    else:
        tickers = [t.strip().upper() for t in tickers_in if t.strip()]

    tickers = list(dict.fromkeys(tickers))  # deduplicate, preserve order
    if not tickers:
        return jsonify({'error': '請選擇要掃描的股票'}), 400
    if len(tickers) > 150:
        return jsonify({'error': '最多一次掃描 150 檔'}), 400

    try:
        results = []
        with ThreadPoolExecutor(max_workers=12) as ex:
            futs = {ex.submit(_scan_ticker, t, conditions, is_tw, period, interval): t for t in tickers}
            for f in as_completed(futs):
                try:
                    r = f.result()
                    if r:
                        results.append(r)
                except Exception:
                    pass

        results.sort(key=lambda x: (x.get('changePct') or 0), reverse=True)
        return jsonify({'results': results, 'total': len(tickers), 'matched': len(results)})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': f'掃描發生錯誤，請稍後再試（{str(e)[:80]}）'}), 500


@app.route('/api/screener/strategies', methods=['GET'])
def screener_strategies_get():
    with _strat_lock:
        return jsonify(_load_strategies())


@app.route('/api/screener/strategies', methods=['POST'])
def screener_strategies_save():
    data = request.json or {}
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'error': 'name required'}), 400
    with _strat_lock:
        s = _load_strategies()
        s[name] = {
            'conditions': data.get('conditions', []),
            'tickers':    data.get('tickers', []),
            'isTw':       data.get('isTw', True),
            'period':     data.get('period', '1y'),
            'interval':   data.get('interval', '1d'),
            'exitAlerts': data.get('exitAlerts', []),
            'savedAt':    pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M'),
        }
        _save_strategies(s)
    return jsonify({'ok': True})


@app.route('/api/screener/strategies/<name>', methods=['DELETE'])
def screener_strategies_delete(name):
    with _strat_lock:
        s = _load_strategies()
        s.pop(name, None)
        _save_strategies(s)
    return jsonify({'ok': True})


# ════════════════════════════════════════════════════════════════════
#  績優股清單（財報狗選股法）：自由現金流報酬率 + P/B + P/E + 殖利率多因子排名
# ════════════════════════════════════════════════════════════════════
def _stmt_series(df, labels):
    """從 yfinance 財報 DataFrame 取出第一個命中欄位，回傳 {年份: 值}（年份為 int）。"""
    out = {}
    if df is None or getattr(df, 'empty', True):
        return out
    for lbl in labels:
        if lbl in df.index:
            for col in df.columns:
                try:
                    yr = int(col.year)
                except Exception:
                    try:    yr = int(str(col)[:4])
                    except Exception: continue
                v = safe_float(df.loc[lbl, col])
                if v != 0:
                    out[yr] = v
            if out:
                return out
    return out


def _norm_div_yield(info):
    """yfinance 的 dividendYield 在不同版本有時是小數(0.025)有時是百分比(2.5)，這裡統一成百分比。"""
    price = safe_float(info.get('currentPrice', info.get('regularMarketPrice', 0)))
    rate  = safe_float(info.get('dividendRate', info.get('trailingAnnualDividendRate', 0)))
    if rate and price:                       # 最可靠：現金股利 / 股價
        return round(rate / price * 100, 2)
    dy = safe_float(info.get('dividendYield', 0))
    if dy <= 0:
        return 0.0
    return round(dy if dy > 1 else dy * 100, 2)   # >1 視為已是百分比


def _bluechip_metrics(ticker, is_tw):
    """計算單一個股的財報狗因子；資料慢變，快取 6 小時。"""
    key = f'bluechip:{ticker}'
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        stock = yf.Ticker(ticker)
        info  = stock.info or {}
        cf    = stock.cashflow
        bs    = stock.balance_sheet

        # ── 各年度自由現金流 = 營業現金流 - 資本支出 ──
        ocf   = _stmt_series(cf, ['Operating Cash Flow', 'Total Cash From Operating Activities'])
        capex = _stmt_series(cf, ['Capital Expenditure', 'Capital Expenditures'])
        fcf_d = _stmt_series(cf, ['Free Cash Flow'])
        fcf_by_year = {}
        for y in set(list(ocf) + list(capex) + list(fcf_d)):
            if y in fcf_d:
                fcf_by_year[y] = fcf_d[y]
            elif y in ocf:
                fcf_by_year[y] = ocf[y] + capex.get(y, 0)   # yfinance 的 capex 已是負值

        # ── 各年度投入資本 = 股東權益 + 長短期金融負債 ──
        equity = _stmt_series(bs, ['Stockholders Equity', 'Total Stockholder Equity', 'Common Stock Equity'])
        tdebt  = _stmt_series(bs, ['Total Debt'])
        ltd    = _stmt_series(bs, ['Long Term Debt'])
        std    = _stmt_series(bs, ['Current Debt', 'Short Long Term Debt',
                                   'Current Debt And Capital Lease Obligation'])
        invcap_by_year = {}
        for y, eq in equity.items():
            debt = tdebt[y] if y in tdebt else (ltd.get(y, 0) + std.get(y, 0))
            ic = eq + debt
            if ic > 0:
                invcap_by_year[y] = ic

        # ── 各年度自由現金流報酬率（新到舊）──
        years = sorted(set(fcf_by_year) & set(invcap_by_year), reverse=True)
        rates = [(y, round(fcf_by_year[y] / invcap_by_year[y] * 100, 2)) for y in years]

        latest_rate = rates[0][1] if rates else None
        prev_rate   = rates[1][1] if len(rates) > 1 else None
        declining   = (latest_rate is not None and prev_rate is not None and latest_rate < prev_rate)

        # 3 年平均自由現金流報酬率（全正用幾何平均，否則算術平均）
        last3 = [r for _, r in rates[:3]]
        if last3:
            if all(r > 0 for r in last3):
                avg3 = round(float(np.prod(last3)) ** (1.0 / len(last3)), 2)
            else:
                avg3 = round(sum(last3) / len(last3), 2)
        else:
            avg3 = None

        # ── 估值因子 ──
        pb = safe_float(info.get('priceToBook', 0))
        pe = safe_float(info.get('trailingPE', 0)) or safe_float(info.get('forwardPE', 0))
        divy = _norm_div_yield(info)
        price = safe_float(info.get('currentPrice', info.get('regularMarketPrice', 0)))
        en_name = (info.get('shortName') or info.get('longName') or ticker)[:30]
        name = tw_cn_name(ticker, en_name) if is_tw else en_name

        result = {
            'ticker':     ticker,
            'display':    tw_display(ticker) if is_tw else ticker,
            'name':       name,
            'price':      round(price, 2),
            'fcfRate':    latest_rate,         # 最新年度自由現金流報酬率 %
            'fcfPrev':    prev_rate,
            'fcfAvg3':    avg3,                # 3 年平均
            'fcfYears':   len(rates),
            'declining':  declining,
            'pb':         round(pb, 2) if pb > 0 else None,
            'pe':         round(pe, 1) if pe > 0 else None,
            'divYield':   divy if divy > 0 else None,
        }
        _cache_set(key, result, ttl=21600)
        return result
    except Exception:
        return None


@app.route('/bluechip')
def bluechip_page():
    from flask import make_response
    resp = make_response(render_template('bluechip.html'))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    return resp


@app.route('/api/bluechip/run', methods=['POST'])
def bluechip_run():
    data        = request.json or {}
    tickers_in  = data.get('tickers', [])
    is_tw       = data.get('isTw', True)
    exclude_dec = data.get('excludeDeclining', True)
    fcf_top_pct = safe_float(data.get('fcfTopPct', 100)) or 100
    top_n       = int(safe_float(data.get('topN', 80)) or 80)

    if is_tw:
        tickers = [tw_normalize(t.strip()) for t in tickers_in if t.strip()]
    else:
        tickers = [t.strip().upper() for t in tickers_in if t.strip()]
    tickers = list(dict.fromkeys(tickers))
    if not tickers:
        return jsonify({'error': '請選擇要排名的股票'}), 400
    if len(tickers) > 120:
        return jsonify({'error': '最多一次排名 120 檔（多因子需抓多年財報，較耗時）'}), 400

    try:
        metrics = []
        with ThreadPoolExecutor(max_workers=10) as ex:
            futs = {ex.submit(_bluechip_metrics, t, is_tw): t for t in tickers}
            for f in as_completed(futs):
                try:
                    m = f.result()
                    if m:
                        metrics.append(m)
                except Exception:
                    pass

        total_fetched = len(metrics)
        excluded = {'noData': 0, 'declining': 0, 'fcfPct': 0}

        # 步驟1：排除自由現金流報酬率下滑的公司
        survivors = []
        for m in metrics:
            if m['fcfAvg3'] is None or m['fcfRate'] is None:
                excluded['noData'] += 1
                continue
            if exclude_dec and m['declining']:
                excluded['declining'] += 1
                continue
            survivors.append(m)

        # 步驟2：依 3 年平均自由現金流報酬率排名，保留前 fcf_top_pct%
        survivors.sort(key=lambda x: x['fcfAvg3'], reverse=True)
        if fcf_top_pct < 100 and survivors:
            keep = max(1, int(round(len(survivors) * fcf_top_pct / 100.0)))
            excluded['fcfPct'] = len(survivors) - keep
            survivors = survivors[:keep]

        # 步驟3/4/5：分別依 P/B(小→大)、P/E(小→大)、殖利率(大→小)排名
        def assign_rank(items, field, reverse, rank_key):
            valid = [it for it in items if it.get(field) is not None]
            valid.sort(key=lambda x: x[field], reverse=reverse)
            for i, it in enumerate(valid):
                it[rank_key] = i + 1
            worst = len(valid) + 1
            for it in items:
                if it.get(rank_key) is None:
                    it[rank_key] = worst        # 缺值排最後

        for m in survivors:
            m['rankPB'] = m['rankPE'] = m['rankDivY'] = None
        assign_rank(survivors, 'pb',       False, 'rankPB')
        assign_rank(survivors, 'pe',       False, 'rankPE')
        assign_rank(survivors, 'divYield', True,  'rankDivY')

        # 步驟6：綜合排名（分數越小越被低估）
        for m in survivors:
            m['totalScore'] = m['rankPB'] + m['rankPE'] + m['rankDivY']
        survivors.sort(key=lambda x: (x['totalScore'], x['rankPB']))
        ranked = survivors[:top_n]
        for i, m in enumerate(ranked):
            m['rank'] = i + 1

        return jsonify({
            'results':  ranked,
            'total':    len(tickers),
            'fetched':  total_fetched,
            'survived': len(survivors),
            'excluded': excluded,
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': f'排名發生錯誤，請稍後再試（{str(e)[:80]}）'}), 500


# ════════════════════════════════════════════════════════════════════
#  低位階長期永動機投資法（春燕來了式）：低位階選股 + 起漲/出場訊號（台股）
# ════════════════════════════════════════════════════════════════════
def _weekly_kd_low(hist):
    """週KD（春燕重視月/週KD低檔）。回傳 (週K, 週D, 是否低檔<40)。"""
    try:
        wk = hist.resample('W').agg({'High': 'max', 'Low': 'min', 'Close': 'last'}).dropna()
        if len(wk) < 12:
            return (None, None, False)
        wk_k, wk_d = calc_kd(wk['High'], wk['Low'], wk['Close'])
        k = safe_float(wk_k.iloc[-1]); d = safe_float(wk_d.iloc[-1])
        return (round(k, 1), round(d, 1), k < 40)
    except Exception:
        return (None, None, False)


def _perpetual_metrics(ticker):
    """輕量版：算位階、距高點回檔、起漲分數，供選股掃描用。快取 15 分鐘。"""
    key = f'perpv2:{ticker}'        # v2：改用 5 年位階＋年線＋週KD（資料結構變更，換鍵避免吃到舊快取）
    c = _cache_get(key)
    if c is not None:
        return c
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='5y', interval='1d')   # 5 年資料，貼近春燕「相對歷史底部」位階
        if hist.empty:
            return None
        hist = hist.dropna(subset=['Close'])         # 對齊各序列長度（避免今日 NaN 造成 calc_kd 長度不符）
        if len(hist) < 60:
            return None
        close = hist['Close']; high = hist['High']; low = hist['Low']; vol = hist['Volume']
        price = safe_float(close.iloc[-1])
        if price <= 0:
            return None
        ma20 = safe_float(close.rolling(20).mean().iloc[-1])
        ma60 = safe_float(close.rolling(60).mean().iloc[-1])
        ma20_5ago = safe_float(close.rolling(20).mean().iloc[-6]) if len(close) > 25 else ma20
        ma20_up = ma20 > ma20_5ago
        ma240 = safe_float(close.rolling(240).mean().iloc[-1]) if len(close) >= 240 else ma60
        below_year = bool(price < ma240)             # 年線之下＝相對低位階
        # 位階：用可取得的全部歷史（最長 5 年）相對高低；距 52 週高點回檔
        hi_all = safe_float(close.max()); lo_all = safe_float(close.min())
        pos = (price - lo_all) / (hi_all - lo_all) * 100 if hi_all > lo_all else 50
        win = close.iloc[-252:] if len(close) >= 252 else close
        hi52 = safe_float(win.max())
        drawdown = (price - hi52) / hi52 * 100 if hi52 else 0
        avgv = safe_float(vol.rolling(20).mean().iloc[-1])
        volr = safe_float(vol.iloc[-1]) / avgv if avgv > 0 else 0
        _, _, osc_s = calc_macd(close)
        osc = safe_float(osc_s.iloc[-1]); osc_prev = safe_float(osc_s.iloc[-2])
        k_s, d_s = calc_kd(high, low, close)
        k = safe_float(k_s.iloc[-1]); d = safe_float(d_s.iloc[-1])
        kp = safe_float(k_s.iloc[-2]); dp = safe_float(d_s.iloc[-2])
        wk_k, wk_d, week_low = _weekly_kd_low(hist)
        rise_factors = [price > ma20, ma20_up, volr >= 1.3, osc > osc_prev, (k > d and kp <= dp)]
        rise = sum(1 for v in rise_factors if v)
        code = ticker.replace('.TW', '').replace('.TWO', '')
        result = {
            'ticker': ticker, 'display': code, 'name': tw_cn_name(ticker, code),
            'price': round(price, 2), 'pos': round(pos, 1), 'drawdown': round(drawdown, 1),
            'ma20': round(ma20, 2), 'ma60': round(ma60, 2), 'ma20Up': ma20_up,
            'belowYear': below_year, 'weekK': wk_k, 'weekLow': bool(week_low),
            'volRatio': round(volr, 2), 'riseScore': rise, 'aboveMa20': bool(price > ma20),
        }
        _cache_set(key, result, ttl=900)
        return result
    except Exception:
        return None


def _perpetual_signal(stock, ticker, price, name, holding=None):
    """低位階永動機訊號：低位階+起漲→進場；持倉則續抱/減碼/出場/停損。
    回傳結構與 _steady_signal 一致，action 用 BUY/SELL/AVOID 接既有推播去重邏輯。"""
    hist = stock.history(period='5y', interval='1d')   # 多年資料以判斷相對歷史位階
    if hist.empty or len(hist) < 60:
        return _signal_wait(ticker, name, price, 'perpetual', '歷史資料不足，無法判斷')
    hist = hist.dropna(subset=['Close'])             # 對齊各序列長度
    if len(hist) < 60:
        return _signal_wait(ticker, name, price, 'perpetual', '歷史資料不足，無法判斷')
    close = hist['Close']; high = hist['High']; low = hist['Low']; vol = hist['Volume']

    ma20 = safe_float(close.rolling(20).mean().iloc[-1])
    ma60 = safe_float(close.rolling(60).mean().iloc[-1])
    ma20_5ago = safe_float(close.rolling(20).mean().iloc[-6]) if len(close) > 25 else ma20
    ma20_up = ma20 > ma20_5ago
    ma240 = safe_float(close.rolling(240).mean().iloc[-1]) if len(close) >= 240 else ma60
    below_year = price < ma240
    hi_all = safe_float(close.max()); lo_all = safe_float(close.min())
    pos = (price - lo_all) / (hi_all - lo_all) * 100 if hi_all > lo_all else 50
    win  = close.iloc[-252:] if len(close) >= 252 else close
    hi52 = safe_float(win.max())
    drawdown = (price - hi52) / hi52 * 100 if hi52 else 0
    avgv = safe_float(vol.rolling(20).mean().iloc[-1])
    volr = safe_float(vol.iloc[-1]) / avgv if avgv > 0 else 0
    _, _, osc_s = calc_macd(close)
    osc = safe_float(osc_s.iloc[-1]); osc_prev = safe_float(osc_s.iloc[-2])
    k_s, d_s = calc_kd(high, low, close)
    k = safe_float(k_s.iloc[-1]); d = safe_float(d_s.iloc[-1])
    kp = safe_float(k_s.iloc[-2]); dp = safe_float(d_s.iloc[-2])
    _wk_k, _wk_d, week_low = _weekly_kd_low(hist)     # 春燕重視週KD低檔
    kd_gold = k > d and kp <= dp
    kd_dead_high = k < d and kp >= dp and k > 70
    above20 = price > ma20
    broke20 = price < ma20 * 0.99
    low_pos = pos < 45 or below_year                 # 位階低 或 年線之下＝相對低位階

    entry_factors = {
        '站上月線': above20, '月線翻揚': ma20_up, '帶量(量比≥1.3)': volr >= 1.3,
        'MACD動能轉強': osc > osc_prev, 'KD黃金交叉': kd_gold,
    }
    entry_score = sum(1 for v in entry_factors.values() if v)
    stop_loss = round(min(ma60, safe_float(low.iloc[-20:].min())) * 0.97, 2)
    ts = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M')
    details = {
        'pos': round(pos, 1), 'drawdown': round(drawdown, 1),
        'ma20': round(ma20, 2), 'ma60': round(ma60, 2), 'ma20Up': ma20_up,
        'volRatio': round(volr, 2), 'entryScore': entry_score,
        'entryFactors': entry_factors, 'k': round(k, 1), 'd': round(d, 1),
    }

    def out(action, action_cn, conf, reason):
        return {'ticker': ticker, 'name': name, 'price': round(price, 2),
                'profile': 'perpetual', 'action': action, 'actionCn': action_cn,
                'confidence': conf, 'reason': reason, 'stopLoss': stop_loss,
                'trailingStop': f'跌破月線 {ma20:.2f} 或停損 {stop_loss:.2f} 時減碼／出場',
                'details': details, 'timeframe': '日K（低位階永動機）', 'timestamp': ts}

    if holding:
        buy_p = safe_float(holding.get('buy_price', 0))
        pnl = (price - buy_p) / buy_p * 100 if buy_p else 0
        details['cost'] = buy_p; details['pnl'] = round(pnl, 2)
        if buy_p and pnl <= -20:                      # 春燕紀律：-20% 為極限，務必停損
            return out('AVOID', '觸及停損上限(-20%)，務必出場', '高',
                f'股價 {price:.2f} 較成本 {buy_p:g} 已虧損 {pnl:.1f}%，達 -20% 停損極限。'
                f'永動機紀律不凹單，立即停損、保留資金等下一檔低位階起漲。')
        if broke20 and buy_p and price < buy_p:
            return out('AVOID', '跌破月線且虧損，建議停損', '中',
                f'股價 {price:.2f} 跌破月線 {ma20:.2f}，且低於成本 {buy_p:g}（{pnl:+.1f}%）。趨勢轉弱，'
                f'永動機紀律：認賠出場、保留資金等下一檔低位階起漲，不凹單。')
        if kd_dead_high or (broke20 and pnl > 0):
            return out('SELL', '高檔轉弱，獲利了結', '中',
                f'股價 {price:.2f}（{pnl:+.1f}%）出現{"KD 高檔死亡交叉" if kd_dead_high else "跌破月線"}，'
                f'動能轉弱。建議分批了結，把資金轉到新的低位階候選（換股）。')
        if pnl >= 24.95:
            return out('SELL', '達停利目標，分批了結', '中',
                f'股價 {price:.2f} 獲利 +{pnl:.1f}% 已達停利區。趨勢仍多可留部分，建議分批落袋，'
                f'釋出資金投入新的低位階起漲股。')
        if low_pos and above20 and entry_score >= 4 and pnl < 10:
            return out('BUY', '低位階加碼', '中',
                f'持倉仍在低位階（{pos:.0f}%）、未明顯起漲（{pnl:+.1f}%），且起漲訊號轉強'
                f'（{entry_score}/5）。可考慮分批加碼攤平成本、放大部位，停損仍守 {stop_loss:.2f}。')
        return out('WAIT', '續抱', '-',
            f'股價 {price:.2f}（{pnl:+.1f}%）守穩月線 {ma20:.2f}，{"月線上揚" if ma20_up else "月線走平"}，'
            f'趨勢未轉弱，續抱。跌破月線或停損 {stop_loss:.2f} 再出場。')

    if low_pos and entry_score >= 3:
        conf = '高' if entry_score >= 4 else '中'
        hit = '、'.join([kk for kk, vv in entry_factors.items() if vv])
        return out('BUY', '低位階起漲！建議進場', conf,
            f'位階僅 {pos:.0f}%（距 52 週高點 {drawdown:.0f}%），出現起漲訊號（{entry_score}/5：{hit}）。'
            f'屬永動機左側低接後轉強，可進場，停損設 {stop_loss:.2f}。')
    if low_pos and entry_score >= 1:
        return out('WATCH', '低位階整理，留意起漲', '低',
            f'位階 {pos:.0f}% 偏低，但起漲訊號未足（{entry_score}/5）。納入監測，等站上月線並帶量再進場。')
    if not low_pos:
        return out('WAIT', '位階偏高，不宜追高', '-',
            f'位階已達 {pos:.0f}%，非低位階區。永動機原則不追高，等拉回低位階再評估。')
    return out('WAIT', '觀望', '-', f'位階 {pos:.0f}%，尚無明確起漲訊號。')


@app.route('/perpetual')
def perpetual_page():
    from flask import make_response
    resp = make_response(render_template('perpetual.html'))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    return resp


@app.route('/api/perpetual/screen', methods=['POST'])
def perpetual_screen():
    data       = request.json or {}
    tickers_in = data.get('tickers', [])
    pos_max    = safe_float(data.get('posMax', 40)) or 40
    dd_min     = safe_float(data.get('drawdownMin', 0))   # 距高點回檔至少 X%（0=不限）
    min_rise   = int(safe_float(data.get('minRise', 0)))
    quality    = bool(data.get('quality', False))
    chip       = bool(data.get('chip', False))

    tickers = list(dict.fromkeys(tw_normalize(t.strip()) for t in tickers_in if t.strip()))
    if not tickers:
        return jsonify({'error': '請選擇要掃描的股票'}), 400
    if len(tickers) > 120:
        return jsonify({'error': '最多一次掃描 120 檔'}), 400

    try:
        metrics = []
        with ThreadPoolExecutor(max_workers=10) as ex:
            futs = {ex.submit(_perpetual_metrics, t): t for t in tickers}
            for f in as_completed(futs):
                try:
                    m = f.result()
                    if m:
                        metrics.append(m)
                except Exception:
                    pass
        fetched = len(metrics)

        cands = []
        for m in metrics:
            is_low = m['pos'] <= pos_max
            is_dd  = dd_min > 0 and m['drawdown'] <= -dd_min
            if not (is_low or is_dd):
                continue
            if m['riseScore'] < min_rise:
                continue
            cands.append(m)

        if quality and cands:             # 績優股基本面過濾：剔除自由現金流轉差/虧損地雷（並行抓取）
            with ThreadPoolExecutor(max_workers=10) as ex:
                bms = list(ex.map(lambda m: _bluechip_metrics(m['ticker'], True), cands))
            kept = []
            for m, bm in zip(cands, bms):
                if bm and bm.get('fcfAvg3') is not None and not bm.get('declining'):
                    m['fcfAvg3'] = bm.get('fcfAvg3'); m['pb'] = bm.get('pb')
                    kept.append(m)
            cands = kept

        if chip and cands:                # 籌碼過濾：三大法人合計買超（並行抓取）
            with ThreadPoolExecutor(max_workers=10) as ex:
                insts = list(ex.map(lambda m: _get_tw_inst(m['display']), cands))
            kept = []
            for m, inst in zip(cands, insts):
                if inst and inst.get('total_net', 0) > 0:
                    m['instNet'] = inst.get('total_net'); kept.append(m)
            cands = kept

        cands.sort(key=lambda x: (-x['riseScore'], x['pos']))
        return jsonify({'results': cands, 'total': len(tickers),
                        'fetched': fetched, 'matched': len(cands)})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': f'掃描發生錯誤，請稍後再試（{str(e)[:80]}）'}), 500


@app.route('/api/perpetual/monitor')
def perpetual_monitor():
    """回傳監測中（profile=perpetual）的標的與最新訊號，供永動機頁的監測區顯示。"""
    with _monitor_lock:
        cfg = _load_monitor_cfg()
    holdings = {h.get('code', '').strip().upper(): h
                for h in _load_agent_cfg().get('holdings', []) if h.get('code')}
    out = []
    for ticker, s in cfg.get('tickers', {}).items():
        if s.get('profile') != 'perpetual':
            continue
        code = ticker.replace('.TW', '').replace('.TWO', '')
        sig = s.get('last_signal') or {}
        h = holdings.get(code)
        out.append({
            'ticker': ticker, 'display': code, 'name': sig.get('name', code),
            'enabled': s.get('enabled', True),
            'action': sig.get('action', 'WAIT'), 'actionCn': sig.get('actionCn', '尚未掃描'),
            'confidence': sig.get('confidence', '-'), 'price': sig.get('price'),
            'reason': sig.get('reason', ''), 'lastScan': s.get('last_scan', ''),
            'cost': (h or {}).get('buy_price'), 'shares': (h or {}).get('shares'),
        })
    return jsonify({'tickers': out, 'paused': bool(cfg.get('monitor_paused', False))})


@app.route('/api/screener/add_alert', methods=['POST'])
def screener_add_alert():
    """Add a ticker to monitor with custom exit alert conditions."""
    data   = request.json or {}
    ticker_raw    = data.get('ticker', '').strip()
    exit_conds    = data.get('exitConditions', [])
    line_token    = data.get('line_token', '')
    line_user_id  = data.get('line_user_id', '')
    is_tw         = data.get('isTw', True)

    if not ticker_raw:
        return jsonify({'error': 'ticker required'}), 400

    ticker = tw_normalize(ticker_raw) if is_tw else ticker_raw.upper()
    now_str = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M')

    with _monitor_lock:
        cfg = _load_monitor_cfg()
        existing = cfg['tickers'].get(ticker, {})
        # Preserve existing keys, add/update exit conditions
        cfg['tickers'][ticker] = {
            'profile':        existing.get('profile', 'steady'),
            'line_token':     line_token or existing.get('line_token', cfg.get('line_token', '')),
            'line_user_id':   line_user_id or existing.get('line_user_id', cfg.get('line_user_id', '')),
            'last_signal':    existing.get('last_signal'),
            'last_scan':      existing.get('last_scan', ''),
            'last_notify_time': existing.get('last_notify_time', ''),
            'registered_at':  existing.get('registered_at', now_str),
            'exit_conditions': exit_conds,
            'exit_last_alert': existing.get('exit_last_alert', {}),
        }
        _save_monitor_cfg(cfg)
    return jsonify({'ok': True, 'ticker': ticker})


def _check_exit_alerts(stock, ticker, price, entry):
    """Check exit conditions and return list of triggered alerts."""
    exit_conds = entry.get('exit_conditions', [])
    if not exit_conds:
        return []

    triggered = []
    try:
        hist = stock.history(period='3mo')
        if hist.empty or len(hist) < 5:
            return []
        info = stock.info
        for cond in exit_conds:
            passed, detail = _eval_condition(hist, info, cond, None)
            if passed:
                label = cond.get('label', cond.get('type', ''))
                triggered.append(f'【出場警示】{label}：{detail}')
    except Exception as e:
        print(f'[ExitAlert] {ticker}: {e}')
    return triggered


# Extend server scan to check exit alerts
_orig_run_server_scan = _run_server_scan

def _run_server_scan_with_exit():
    _orig_run_server_scan()
    # Check exit alerts
    with _monitor_lock:
        cfg = _load_monitor_cfg()
    if cfg.get('monitor_paused'):
        return
    now_str = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M')
    for ticker, entry in list(cfg.get('tickers', {}).items()):
        if not entry.get('enabled', True):
            continue
        if not entry.get('exit_conditions'):
            continue
        # 非交易時段不推播出場警示，避免半夜／收盤後一直發
        if not _ticker_session_open(ticker):
            continue
        try:
            stock = yf.Ticker(ticker)
            info  = stock.info
            price = safe_float(info.get('currentPrice', info.get('regularMarketPrice', 0)))
            if price <= 0:
                continue
            alerts = _check_exit_alerts(stock, ticker, price, entry)
            if not alerts:
                continue
            # Cooldown: don't spam same exit alert within 4 hours
            last_alerts = entry.get('exit_last_alert', {})
            line_token   = entry.get('line_token', '')
            line_user_id = entry.get('line_user_id', '')
            for alert_text in alerts:
                key = alert_text[:40]
                last_t = last_alerts.get(key, '')
                cooldown_ok = not last_t or (
                    pd.Timestamp.now(tz='Asia/Taipei') -
                    pd.Timestamp(last_t, tz='Asia/Taipei')).total_seconds() > 14400
                if cooldown_ok and line_token and line_user_id:
                    name = tw_cn_name(ticker, info.get('shortName', ticker))
                    msg  = f'【{name}】{alert_text}\n現價: {price}\n時間: {now_str}'
                    _push_line_msg(line_token, line_user_id, msg)
                    with _monitor_lock:
                        cfg2 = _load_monitor_cfg()
                        if ticker in cfg2['tickers']:
                            cfg2['tickers'][ticker].setdefault('exit_last_alert', {})[key] = now_str
                            _save_monitor_cfg(cfg2)
        except Exception as e:
            print(f'[ExitAlert scan] {ticker}: {e}')

# Replace the scan function used by the loop
import sys
sys.modules[__name__].__dict__['_run_server_scan'] = _run_server_scan_with_exit


# ═══════════════════════════════════════════════════════════════════════════
#  明日預測模組 — AI 分析師
# ═══════════════════════════════════════════════════════════════════════════

_PREDICT_SYSTEM_PROMPT = """你是一位精通台股技術分析與籌碼追蹤的專業交易員助理。
你會收到使用者提供的個股代碼，系統會自動取得以下數據供你分析：

【均線扣抵環境】
- 20MA（月線）當前值與明日扣抵預估值
- 最新收盤價 vs 明日扣抵價，判斷月線走向

【五大起漲籌碼防禦指標】
1. 量能結構：5MV 是否站上 20MV，且 20MV 走平或上揚
2. 借券動向：借券賣出餘額是否連續減少（空方回補）
3. 乖離位階：股價在 20MA 之上且乖離率 < 6%（剛轉強未過熱）
4. 反彈幅度：從近期低點反彈 < 15%（仍處低檔初升段）
5. 籌碼穩定度：當沖比 < 40%（主力籌碼穩定）

【量價動能】KD、MACD/OSC、成交量爆量或窒息量

【法人籌碼】外資、投信、自營商買賣超

【近期新聞】可能影響股價的事件

分析時請嚴格依照以下格式輸出：

### 綜合趨勢評估
- **基本基調：** 偏多上攻 / 震盪洗盤 / 偏空回檔
- **五大起漲指標符合度：** X/5 項（列出符合與不符合的項目）

### 明日實戰預測
- **預估漲跌幅區間：** 例如 +1.5% ~ +3.0%
- **多方攻擊關鍵壓力價：** [價位 + 技術依據]
- **空方防守關鍵支撐價：** [價位 + 技術依據]

### 各指標詳細分析
（逐項說明均線扣抵、量能、借券、KD、MACD、法人）

### 主要風險
（列出 2-3 點需要注意的風險）

回應請使用繁體中文，語氣專業簡潔，全程不要使用任何 emoji 或表情符號。若使用者沒有提供股票代碼，請主動詢問並引導使用者輸入台股代碼（如 2330、0050 等）。"""


def _multi_factor_score(d: dict) -> dict:
    """
    15 因子多維度評分系統，對齊主流券商選股邏輯。
    回傳 dict: {total, max, grade, breakdown: [{name, pass, weight, detail}]}
    """
    factors = []

    def add(name: str, passed: bool, weight: int, detail: str):
        factors.append({'name': name, 'pass': passed, 'weight': weight, 'detail': detail})

    price  = d.get('price', 0)
    ma5    = d.get('ma5', 0)
    ma20   = d.get('ma20', 0)
    ma60   = d.get('ma60', 0)
    k_val  = d.get('k', 50)
    d_val  = d.get('d', 50)
    wk     = d.get('week_k', 50)
    osc    = d.get('osc', 0)
    mv5    = d.get('mv5', 0)
    mv20   = d.get('mv20', 1)
    bias   = d.get('bias20', 99)
    rbd    = d.get('rebound_pct', 99)
    dtr    = d.get('day_trade_ratio')
    bb_pct = d.get('bb_pct', 50)
    w52p   = d.get('w52_pct', 50)
    vol_r  = d.get('vol_ratio', 1.0)
    inst   = d.get('inst') or {}
    mg     = d.get('margin') or {}
    candle = d.get('last_candle', 'black')
    deduct = d.get('ma20_deduct_price', 9999)

    # ── 1. 均線多頭排列（MA5 > MA20 > MA60）── weight 2
    bull = bool(ma5 > 0 and ma20 > 0 and ma60 > 0 and ma5 > ma20 > ma60)
    add('均線多頭排列', bull, 2,
        'MA5>MA20>MA60 多頭格局' if bull else f'MA5={ma5} MA20={ma20} MA60={ma60}，排列不佳')

    # ── 2. 月線扣抵向上（收盤 > 扣抵價）── weight 2
    deduct_ok = price > deduct
    add('月線扣抵向上', deduct_ok, 2,
        f'現價{price} > 扣抵{deduct}，月線上揚' if deduct_ok else f'現價{price} < 扣抵{deduct}，月線下彎壓力')

    # ── 3. KD 黃金交叉或低檔向上── weight 2
    kd_ok = (k_val < 50 and k_val > d_val) or (20 < k_val < 70 and k_val > d_val)
    add('KD 低檔向上', kd_ok, 2,
        f'K={k_val} > D={d_val}，動能上行' if kd_ok else f'K={k_val} D={d_val}，K<D 或高檔')

    # ── 4. 週 KD 低檔（週 K < 50 向上）── weight 1
    week_kd_ok = wk < 50
    add('週 KD 低檔', week_kd_ok, 1,
        f'週K={wk}，中長期低檔' if week_kd_ok else f'週K={wk}，中長期偏高')

    # ── 5. MACD 紅柱放大── weight 2
    macd_ok = osc > 0
    add('MACD 紅柱', macd_ok, 2,
        f'OSC={osc}，多頭動能' if macd_ok else f'OSC={osc}，空頭或觀望')

    # ── 6. 量能健康（5MV > 20MV 且量比 > 0.8）── weight 1
    vol_ok = mv5 > mv20 and vol_r >= 0.8
    add('量能健康', vol_ok, 1,
        f'5MV({mv5:,})>20MV({mv20:,})，量比{vol_r}x' if vol_ok else f'量能偏弱')

    # ── 7. 爆量收紅（量比 > 1.5 且收紅）── weight 1
    vol_burst = vol_r >= 1.5 and candle == 'red'
    add('爆量收紅', vol_burst, 1,
        f'量比{vol_r}x 且收紅，主力進場' if vol_burst else f'量比{vol_r}x {"收紅" if candle=="red" else "收黑"}')

    # ── 8. 乖離率安全（0 < 乖離 < 6%，剛轉強未過熱）── weight 1
    bias_ok = 0 < bias < 6
    add('乖離未過熱', bias_ok, 1,
        f'乖離{bias}%，站上月線但未過熱' if bias_ok else f'乖離{bias}%，{"過熱注意" if bias >= 6 else "尚未站上月線"}')

    # ── 9. 反彈幅度安全（< 15%，初升段）── weight 1
    rebound_ok = 0 < rbd < 15
    add('反彈初升段', rebound_ok, 1,
        f'反彈{rbd:.1f}%，仍在安全初升段' if rebound_ok else f'反彈{rbd:.1f}%，{"已超漲" if rbd >= 15 else "尚未起漲"}')

    # ── 10. 布林通道位置（bb_pct 20-55%，底部向上）── weight 1
    bb_ok = 20 <= bb_pct <= 55
    add('布林低位起攻', bb_ok, 1,
        f'布林位置{bb_pct}%，中低位起攻區' if bb_ok else f'布林位置{bb_pct}%，{"高位" if bb_pct > 55 else "極低位"}')

    # ── 11. 52 週位階合理（20-60%，非高位追）── weight 1
    pos_ok = 20 <= w52p <= 60
    add('52週低位區', pos_ok, 1,
        f'52週位置{w52p}%，低位佈局區' if pos_ok else f'52週位置{w52p}%，{"高位追價風險" if w52p > 60 else "底部未確認"}')

    # ── 12. 當沖比低（< 40%）── weight 1
    dtr_ok = dtr is None or dtr < 40
    add('籌碼穩定', dtr_ok, 1,
        f'當沖比{"未知" if dtr is None else f"{dtr}%"}，籌碼{"穩定" if dtr_ok else "虛浮"}')

    # ── 13. 三大法人買超── weight 2
    total_net = inst.get('total_net', 0)
    inst_ok   = total_net > 0
    add('法人買超', inst_ok, 2,
        f'法人合計+{total_net/1000:,.0f}張，主力進場' if inst_ok else
        (f'法人合計{total_net/1000:,.0f}張，賣超' if total_net < 0 else '法人資料未取得'))

    # ── 14. 融資籌碼健康（融資減少 or 低水位）── weight 1
    margin_chg = mg.get('margin_chg', 0)
    margin_ok  = margin_chg <= 0
    add('融資籌碼健康', margin_ok, 1,
        f'融資{"減少" if margin_chg < 0 else "持平"}{abs(margin_chg):.0f}張，籌碼乾淨' if margin_ok else
        f'融資增加{margin_chg:.0f}張，散戶追高風險')

    # ── 15. RSI 低檔回升（RSI 30-60 向上）── weight 1
    rsi = d.get('rsi', 50)
    rsi_ok = 30 <= rsi <= 65
    add('RSI 低檔回升', rsi_ok, 1,
        f'RSI={rsi}，超賣回升區' if rsi_ok else f'RSI={rsi}，{"超買" if rsi > 65 else "極度超賣"}')

    total  = sum(f['weight'] for f in factors if f['pass'])
    max_sc = sum(f['weight'] for f in factors)

    pct = total / max_sc * 100 if max_sc else 0
    if pct >= 75:   grade = 'A'
    elif pct >= 58: grade = 'B'
    elif pct >= 42: grade = 'C'
    elif pct >= 25: grade = 'D'
    else:           grade = 'F'

    return {'total': total, 'max': max_sc, 'grade': grade, 'pct': round(pct, 1), 'breakdown': factors}


def _fetch_predict_data(code: str) -> dict:
    """取得股票分析所需的所有數據"""
    import urllib.request, datetime
    ticker_yf = code + '.TW'
    result = {'code': code, 'ticker': ticker_yf, 'error': None}

    try:
        stock = yf.Ticker(ticker_yf)
        hist = stock.history(period='6mo', interval='1d')
        if hist.empty:
            ticker_yf = code + '.TWO'
            stock = yf.Ticker(ticker_yf)
            hist = stock.history(period='6mo', interval='1d')
        if hist.empty:
            result['error'] = f'找不到股票代碼 {code}'
            return result

        close = hist['Close']
        high  = hist['High']
        low   = hist['Low']
        vol   = hist['Volume']
        n     = len(close)

        price = float(close.iloc[-1])
        result['price'] = round(price, 2)
        result['name']  = tw_cn_name(code, code)

        # ── ETF 偵測與專屬資料 ────────────────────────────────
        # 台股 ETF 代碼幾乎都以「00」開頭（0050/0056/00713/00878/00918…）。
        # ETF 不該用個股的本益比、法人、籌碼技術指標來判斷買賣，這裡另抓淨值、
        # 殖利率、費用率、追蹤類別與近期績效，供 ETF 專屬分析使用。
        result['is_etf'] = code.startswith('00')
        if result['is_etf']:
            try:
                info = stock.info
            except Exception:
                info = {}
            etf = {}
            dy = safe_float(info.get('dividendYield', info.get('yield', 0)))
            if 0 < dy < 1:        # yfinance 殖利率有時是小數(0.05)有時是百分比(5.0)
                dy *= 100
            etf['yield']    = round(dy, 2) if dy else None
            nav             = safe_float(info.get('navPrice', 0))
            etf['nav']      = round(nav, 2) if nav else None
            if nav and price:
                etf['premium_pct'] = round((price - nav) / nav * 100, 2)  # 溢價(+)/折價(-)
            etf['category'] = info.get('category') or info.get('legalType') or ''
            etf['family']   = info.get('fundFamily') or ''
            exp             = safe_float(info.get('annualReportExpenseRatio', 0))
            etf['expense']  = round(exp * 100, 3) if exp else None
            ytd             = info.get('ytdReturn')
            etf['ytd_return'] = round(safe_float(ytd) * 100, 2) if ytd is not None else None
            # 近期績效用價格自行計算（一定有資料，不依賴 info）
            try:
                if n >= 21:  etf['ret_1m'] = round((price / float(close.iloc[-21]) - 1) * 100, 2)
                if n >= 63:  etf['ret_3m'] = round((price / float(close.iloc[-63]) - 1) * 100, 2)
                etf['ret_6m'] = round((price / float(close.iloc[0]) - 1) * 100, 2)
            except Exception:
                pass
            result['etf'] = etf

        # ── 均線 ──────────────────────────────────────────────
        ma5  = float(close.rolling(5,  min_periods=1).mean().iloc[-1])
        ma20 = float(close.rolling(20, min_periods=1).mean().iloc[-1])
        ma60 = float(close.rolling(60, min_periods=1).mean().iloc[-1])
        result['ma5']  = round(ma5,  2)
        result['ma20'] = round(ma20, 2)
        result['ma60'] = round(ma60, 2)

        # ── 月線扣抵（明日扣抵值 = 21 個交易日前的收盤價）──────
        deduct_idx = max(0, n - 21)
        deduct_price = float(close.iloc[deduct_idx])
        result['ma20_deduct_price'] = round(deduct_price, 2)
        result['ma20_trend'] = '上揚（支撐防護罩）' if price > deduct_price else '下彎（蓋頭壓力）'

        # ── 乖離率 ────────────────────────────────────────────
        bias = (price - ma20) / ma20 * 100 if ma20 else 0
        result['bias20'] = round(bias, 2)

        # ── 反彈幅度（近 60 日低點）─────────────────────────
        recent_low = float(low.iloc[-60:].min()) if n >= 60 else float(low.min())
        rebound_pct = (price - recent_low) / recent_low * 100 if recent_low else 0
        result['recent_low']   = round(recent_low, 2)
        result['rebound_pct']  = round(rebound_pct, 2)

        # ── 成交量均量 ────────────────────────────────────────
        mv5  = float(vol.rolling(5,  min_periods=1).mean().iloc[-1])
        mv20 = float(vol.rolling(20, min_periods=1).mean().iloc[-1])
        mv20_prev = float(vol.rolling(20, min_periods=1).mean().iloc[-2]) if n > 2 else mv20
        result['mv5']  = int(mv5)
        result['mv20'] = int(mv20)
        result['vol_structure'] = '健康（5MV>20MV，20MV上揚）' if mv5 > mv20 and mv20 >= mv20_prev else (
            '普通（5MV>20MV，但20MV走平或下彎）' if mv5 > mv20 else '偏弱（5MV<20MV）')

        # ── KD ───────────────────────────────────────────────
        k_s, d_s = calc_kd(high, low, close)
        k_val = round(float(k_s.iloc[-1]), 1)
        d_val = round(float(d_s.iloc[-1]), 1)
        result['k'] = k_val
        result['d'] = d_val
        if k_val < 20:   kd_status = '低檔築底'
        elif k_val > 80: kd_status = '高檔鈍化'
        elif k_val > d_val and float(k_s.iloc[-2]) <= float(d_s.iloc[-2]):
            kd_status = '黃金交叉（剛發生）'
        elif k_val < d_val and float(k_s.iloc[-2]) >= float(d_s.iloc[-2]):
            kd_status = '死亡交叉（剛發生）'
        else:
            kd_status = 'K>D 上行' if k_val > d_val else 'K<D 下行'
        result['kd_status'] = kd_status

        # ── MACD ─────────────────────────────────────────────
        macd_s, sig_s, hist_s = calc_macd(close)
        macd_v = round(float(macd_s.iloc[-1]), 3)
        dea_v  = round(float(sig_s.iloc[-1]), 3)
        osc    = round(float(hist_s.iloc[-1]), 3)
        osc_prev = round(float(hist_s.iloc[-2]), 3)
        result['macd']    = macd_v
        result['dea']     = dea_v
        result['osc']     = osc
        result['osc_trend'] = '紅柱放大（動能增強）' if osc > 0 and osc > osc_prev else (
            '紅柱收縮（動能減弱）' if osc > 0 else (
            '綠柱收縮（賣壓減少）' if osc < 0 and osc > osc_prev else '綠柱放大（賣壓增加）'))

        # ── 近日最高最低（支撐壓力）─────────────────────────
        result['high_20d'] = round(float(high.iloc[-20:].max()), 2)
        result['low_20d']  = round(float(low.iloc[-20:].min()), 2)
        result['high_5d']  = round(float(high.iloc[-5:].max()), 2)
        result['low_5d']   = round(float(low.iloc[-5:].min()), 2)

        # ── 法人資料 ─────────────────────────────────────────
        inst = _get_tw_inst(code)
        if inst:
            result['inst'] = {
                'foreign_net': inst.get('foreign_net', 0),
                'trust_net':   inst.get('trust_net', 0),
                'dealer_net':  inst.get('dealer_net', 0),
                'total_net':   inst.get('total_net', 0),
            }
        else:
            result['inst'] = None

        # ── 融資融券 ─────────────────────────────────────────
        mg = _get_tw_margin(code)
        if mg:
            margin_chg = mg['margin_today'] - mg['margin_prev']
            short_chg  = mg['short_today']  - mg['short_prev']
            result['margin'] = {
                'margin_today': mg['margin_today'],
                'margin_chg':   margin_chg,
                'short_today':  mg['short_today'],
                'short_chg':    short_chg,
            }
        else:
            result['margin'] = None

        # ── 借券賣出餘額（FinMind，TWSE 本機被封鎖）──────────
        try:
            result['lending'] = _get_tw_lending(code)
        except Exception:
            result['lending'] = None

        # ── 法人連續買賣超序列（最新在前，供基本面/籌碼面評估）──
        try:
            result['inst_hist'] = _get_tw_inst_hist(code)
        except Exception:
            result['inst_hist'] = None

        # ── 月營收 YoY/MoM（基本面）──────────────────────────
        try:
            result['month_rev'] = _get_tw_month_revenue(code)
        except Exception:
            result['month_rev'] = None

        # ── EPS／最新季度獲利（基本面）───────────────────────
        try:
            result['eps'] = _get_tw_eps(code)
        except Exception:
            result['eps'] = None

        # ── 當沖比（FinMind 當沖量 / 該日總量）────────────────
        try:
            dt = _get_tw_daytrade(code)
            day_trade_ratio = None
            if dt and dt.get('volume', 0) > 0:
                # 取當沖那一日的總成交量（對不到日期則退回最新一日）
                try:
                    volmap = {i.strftime('%Y-%m-%d'): float(v) for i, v in vol.items()}
                    total_vol = volmap.get(dt['date'], float(vol.iloc[-1]))
                except Exception:
                    total_vol = float(vol.iloc[-1])
                if total_vol > 0:
                    day_trade_ratio = round(dt['volume'] / total_vol * 100, 1)
            result['day_trade_ratio'] = day_trade_ratio
        except Exception:
            result['day_trade_ratio'] = None

        # ── 進階技術指標（供多因子評分）─────────────────────
        # RSI
        try:
            rsi_val = round(float(calc_rsi(close).iloc[-1]), 1)
            result['rsi'] = rsi_val
        except Exception:
            result['rsi'] = 50

        # 布林通道
        try:
            bb_u, bb_m, bb_l = calc_bollinger(close)
            bbu = float(bb_u.iloc[-1]); bbl = float(bb_l.iloc[-1]); bbm = float(bb_m.iloc[-1])
            result['bb_upper'] = round(bbu, 2)
            result['bb_lower'] = round(bbl, 2)
            result['bb_mid']   = round(bbm, 2)
            result['bb_pct']   = round((price - bbl) / max(bbu - bbl, 0.01) * 100, 1)
        except Exception:
            result['bb_upper'] = result['bb_lower'] = result['bb_mid'] = 0
            result['bb_pct'] = 50

        # 均線多頭排列
        result['ma_bull'] = bool(ma5 > ma20 > ma60)
        result['price_above_ma20'] = bool(price > ma20)
        result['price_above_ma60'] = bool(price > ma60)

        # 近期量能爆發（最新量 vs 均量）
        vol_latest = float(vol.iloc[-1]) if n >= 1 else 0
        result['vol_ratio'] = round(vol_latest / mv20, 2) if mv20 > 0 else 1.0
        # 最新 K 棒顏色（收紅或收黑）
        open_p = hist['Open']
        result['last_candle'] = 'red' if float(close.iloc[-1]) >= float(open_p.iloc[-1]) else 'black'

        # 週 KD（用週線收盤估算）
        try:
            hist_w = stock.history(period='1y', interval='1wk')
            if len(hist_w) >= 9:
                wk_s, wd_s = calc_kd(hist_w['High'], hist_w['Low'], hist_w['Close'])
                result['week_k'] = round(float(wk_s.iloc[-1]), 1)
                result['week_d'] = round(float(wd_s.iloc[-1]), 1)
            else:
                result['week_k'] = result['week_d'] = 50
        except Exception:
            result['week_k'] = result['week_d'] = 50

        # 52 週高低點位階
        try:
            high52 = float(high.iloc[-252:].max()) if n >= 252 else float(high.max())
            low52  = float(low.iloc[-252:].min())  if n >= 252 else float(low.min())
            result['w52_high'] = round(high52, 2)
            result['w52_low']  = round(low52,  2)
            result['w52_pct']  = round((price - low52) / max(high52 - low52, 0.01) * 100, 1)
        except Exception:
            result['w52_high'] = result['w52_low'] = 0
            result['w52_pct'] = 50

        # 計算多因子評分
        result['mf_score'] = _multi_factor_score(result)

    except Exception as e:
        result['error'] = str(e)

    return result


def _build_analysis_context(data: dict) -> str:
    """將股票數據轉成給 Claude 的分析文字"""
    if data.get('error'):
        return f"錯誤：{data['error']}"

    code  = data['code']
    name  = data.get('name', code)
    price = data.get('price', 0)
    lines = [f"## {name}（{code}）股票分析數據", f"最新收盤價：{price}"]

    # 月線扣抵
    lines.append(f"\n### 【月線扣抵環境】")
    lines.append(f"- 20MA 當前值：{data.get('ma20', 'N/A')}")
    lines.append(f"- 明日扣抵估算價：{data.get('ma20_deduct_price', 'N/A')}")
    lines.append(f"- 月線方向研判：{data.get('ma20_trend', 'N/A')}")
    lines.append(f"- 乖離率（20MA）：{data.get('bias20', 'N/A')}%（{'[符合] <6%，未過熱' if abs(data.get('bias20', 99)) < 6 else '[注意] >6%，注意過熱'}）")

    # 五大指標
    lines.append(f"\n### 【五大起漲籌碼指標】")
    lines.append(f"1. 量能結構：{data.get('vol_structure', 'N/A')}（5MV={data.get('mv5',0):,} vs 20MV={data.get('mv20',0):,}）")

    lending = data.get('lending')
    if lending:
        lines.append(f"2. 借券賣出餘額：今日 {lending.get('lending_balance', 'N/A'):,} 張（借券賣出：{lending.get('lending_sell', 'N/A'):,}）")
    else:
        lines.append(f"2. 借券資料：今日尚未取得或非交易時間")

    rebound = data.get('rebound_pct', 0)
    lines.append(f"3. 乖離位階：乖離 {data.get('bias20', 'N/A')}%（{'[符合] 符合' if 0 < data.get('bias20', 99) < 6 else '[不符] 不符合'}）")
    lines.append(f"4. 反彈幅度：從近期低點 {data.get('recent_low', 'N/A')} 反彈 {rebound:.1f}%（{'[符合] 符合 <15%' if rebound < 15 else '[注意] 已超過15%'}）")

    dtr = data.get('day_trade_ratio')
    if dtr is not None:
        lines.append(f"5. 當沖比：{dtr}%（{'[符合] <40%，籌碼穩定' if dtr < 40 else '[注意] ≥40%，籌碼較虛浮'}）")
    else:
        lines.append(f"5. 當沖比：今日尚未取得（非交易時間或資料延遲）")

    # 技術指標
    lines.append(f"\n### 【技術指標】")
    lines.append(f"- KD：K={data.get('k','N/A')} / D={data.get('d','N/A')} → {data.get('kd_status','N/A')}")
    lines.append(f"- MACD：{data.get('macd','N/A')} / DEA：{data.get('dea','N/A')}")
    lines.append(f"- OSC 柱狀：{data.get('osc','N/A')} → {data.get('osc_trend','N/A')}")

    # 支撐壓力
    lines.append(f"\n### 【近期支撐壓力】")
    lines.append(f"- 近 5 日最高：{data.get('high_5d','N/A')}（短線壓力）")
    lines.append(f"- 近 20 日最高：{data.get('high_20d','N/A')}（中期壓力）")
    lines.append(f"- 近 5 日最低：{data.get('low_5d','N/A')}（短線支撐）")
    lines.append(f"- 近 20 日最低：{data.get('low_20d','N/A')}（中期支撐）")
    lines.append(f"- MA5={data.get('ma5','N/A')} / MA20={data.get('ma20','N/A')} / MA60={data.get('ma60','N/A')}")

    # 法人
    inst = data.get('inst')
    lines.append(f"\n### 【三大法人（今日）】")
    if inst:
        total = inst.get('total_net', 0)
        lines.append(f"- 外資買賣超：{inst.get('foreign_net', 0)/1000:+,.0f} 張")
        lines.append(f"- 投信買賣超：{inst.get('trust_net', 0)/1000:+,.0f} 張")
        lines.append(f"- 自營商買賣超：{inst.get('dealer_net', 0)/1000:+,.0f} 張")
        lines.append(f"- 三大合計：{total/1000:+,.0f} 張（{'主力買超' if total > 0 else '主力賣超'}）")
    else:
        lines.append(f"- 今日法人資料尚未取得")

    # 融資融券
    mg = data.get('margin')
    lines.append(f"\n### 【融資融券】")
    if mg:
        lines.append(f"- 融資餘額：{mg.get('margin_today', 0):,.0f}（{'增加' if mg.get('margin_chg', 0) > 0 else '減少'} {abs(mg.get('margin_chg', 0)):,.0f}）")
        lines.append(f"- 融券餘額：{mg.get('short_today', 0):,.0f}（{'增加' if mg.get('short_chg', 0) > 0 else '減少'} {abs(mg.get('short_chg', 0)):,.0f}）")
    else:
        lines.append(f"- 今日融資融券資料尚未取得")

    return '\n'.join(lines)


@app.route('/predict')
def predict_page():
    return render_template('predict.html')


@app.route('/api/predict/analyze/<code>')
def predict_analyze(code):
    """取得股票分析數據（JSON）"""
    code = code.strip().upper().replace('.TW', '').replace('.TWO', '')
    data = _fetch_predict_data(code)
    return jsonify(data)


@app.route('/api/predict/chat', methods=['POST'])
def predict_chat():
    """Claude AI 聊天端點（串流）"""
    import anthropic
    from flask import Response, stream_with_context

    body = request.get_json(force=True)
    messages   = body.get('messages', [])
    api_key    = body.get('api_key', '').strip()
    stock_code = body.get('stock_code', '').strip()

    if not api_key:
        return jsonify({'error': '請先設定 Claude API Key'}), 400
    if not messages:
        return jsonify({'error': '訊息不可為空'}), 400

    # 如果有股票代碼，自動取得數據並注入到最後一條 user 訊息前
    context_msg = None
    if stock_code:
        code = stock_code.upper().replace('.TW', '').replace('.TWO', '')
        data = _fetch_predict_data(code)
        ctx  = _build_analysis_context(data)
        context_msg = {
            'role':    'user',
            'content': f"請根據以下最新數據分析這支股票：\n\n{ctx}\n\n請依照你的分析框架給出明日漲跌預測。"
        }

    def generate():
        try:
            client = anthropic.Anthropic(api_key=api_key)
            send_msgs = list(messages)
            if context_msg:
                send_msgs = [context_msg] + messages[1:] if len(messages) > 1 else [context_msg]

            with client.messages.stream(
                model='claude-opus-4-8',
                max_tokens=2048,
                system=_PREDICT_SYSTEM_PROMPT,
                messages=send_msgs,
            ) as stream:
                for text in stream.text_stream:
                    yield f"data: {json.dumps({'text': _strip_emoji(text)})}\n\n"
            yield "data: [DONE]\n\n"
        except anthropic.AuthenticationError:
            yield f"data: {json.dumps({'error': 'API Key 無效，請重新確認'})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


_PARSE_HOLDINGS_PROMPT = """你是持倉截圖解析器。使用者上傳的是券商 App／看盤軟體的持倉（庫存）截圖。
請辨識畫面中每一檔股票，輸出「純 JSON 陣列」，不要任何說明文字、不要 markdown 程式碼框。

每個元素格式：
{"code": "股票代碼", "name": "名稱", "shares": 股數整數, "buy_price": 成本均價數字, "market": "TW 或 US"}

規則：
- code：台股只保留數字代碼（例如 2330），美股用英文代碼（例如 AAPL、NVDA）。
- market：台股填 "TW"，美股填 "US"。無法判斷時，純數字代碼視為 TW、英文字母代碼視為 US。
- shares：填「股數」。若截圖單位是「張」，請換算成股數（1 張 = 1000 股）；若顯示零股就照數字。看不到就填 0。
- buy_price：成本價／均價／買入價。看不到就填 0。
- name：看得到就填，看不到填空字串。
- 只輸出實際看得到的持倉，不要捏造。畫面沒有任何持倉時輸出 []。"""


@app.route('/api/parse-holdings-image', methods=['POST'])
def parse_holdings_image_api():
    """用 Claude 視覺解析持倉截圖，回傳結構化持倉清單供前端預覽。"""
    import anthropic
    import base64 as _b64
    import re as _re

    body     = request.get_json(force=True) or {}
    data_url = (body.get('image') or '').strip()
    api_key  = (body.get('api_key') or '').strip() or _agent_api_key()

    if not api_key:
        return jsonify({'error': '尚未設定 Claude API Key，請先到 AI Agent 頁設定金鑰'}), 400
    if not data_url:
        return jsonify({'error': '沒有收到圖片'}), 400

    # 解析 data URL：data:image/png;base64,xxxx
    m = _re.match(r'data:(image/[a-zA-Z+]+);base64,(.+)$', data_url, _re.DOTALL)
    if m:
        media_type, b64data = m.group(1), m.group(2)
    else:
        media_type, b64data = 'image/png', data_url
    media_type = media_type.lower()
    if media_type == 'image/jpg':
        media_type = 'image/jpeg'
    if media_type not in ('image/png', 'image/jpeg', 'image/gif', 'image/webp'):
        media_type = 'image/png'

    # 大小保護：Claude 單張圖上限約 5MB（base64 後）
    if len(b64data) > 7_000_000:
        return jsonify({'error': '圖片太大，請壓縮或裁切後再上傳（建議 < 5MB）'}), 400

    try:
        # 驗證 base64 可解碼
        _b64.b64decode(b64data, validate=True)
    except Exception:
        return jsonify({'error': '圖片資料無效，請重新上傳'}), 400

    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model='claude-opus-4-8',
            max_tokens=2048,
            messages=[{
                'role': 'user',
                'content': [
                    {'type': 'image', 'source': {
                        'type': 'base64', 'media_type': media_type, 'data': b64data}},
                    {'type': 'text', 'text': _PARSE_HOLDINGS_PROMPT},
                ],
            }],
        )
        raw = ''.join(b.text for b in resp.content if getattr(b, 'type', '') == 'text').strip()
    except anthropic.AuthenticationError:
        return jsonify({'error': 'Claude API Key 無效，請重新確認'}), 400
    except Exception as e:
        return jsonify({'error': f'AI 解析失敗：{e}'}), 500

    # 從回覆中擷取 JSON 陣列
    arr_match = _re.search(r'\[.*\]', raw, _re.DOTALL)
    if not arr_match:
        return jsonify({'error': '無法從截圖辨識出持倉，請換一張更清楚的圖', 'raw': raw[:300]}), 422
    try:
        parsed = json.loads(arr_match.group(0))
    except Exception:
        return jsonify({'error': '解析結果格式異常，請重試', 'raw': raw[:300]}), 422

    holdings = []
    for item in parsed if isinstance(parsed, list) else []:
        if not isinstance(item, dict):
            continue
        code = str(item.get('code', '')).strip().upper().replace('.TW', '').replace('.TWO', '')
        if not code:
            continue
        market = str(item.get('market', '')).strip().upper()
        if market not in ('TW', 'US'):
            market = 'TW' if code.isdigit() else 'US'
        holdings.append({
            'code':      code,
            'name':      str(item.get('name', '')).strip()[:40],
            'shares':    safe_int(item.get('shares', 0)),
            'buy_price': safe_float(item.get('buy_price', 0)),
            'market':    market,
        })

    return jsonify({'holdings': holdings, 'count': len(holdings)})


# ═══════════════════════════════════════════════════════════════════════════
#  AI Agent 自動化投資監控模組
# ═══════════════════════════════════════════════════════════════════════════

AGENT_FILE = os.path.join(os.path.dirname(__file__), 'agent_config.json')
_agent_lock = threading.Lock()

_AGENT_SYSTEM_PROMPT = """你是一位專業台股投資 AI 助理，負責分析持倉與市場機會，給出明確的操作建議。

你的職責：
1. 綜合「技術面 + 基本面 + 籌碼面 + 消息面」對持倉股票給出【買進/加碼/持有/減碼/賣出】建議，不要只看技術線型
2. 掃描低價但出現起漲訊號的股票，主動推薦值得關注的標的
3. 每次建議必須說明理由、關鍵支撐壓力價、預估漲跌幅

多面向判斷（系統會在數據中附上以下欄位，請務必納入綜合評估）：
- 基本面：月營收 YoY/MoM（成長動能是否轉強或衰退）、最新季度 EPS／稅後淨利（獲利品質）。營收連月 YoY 轉正且 EPS 成長＝基本面支撐買進；營收衰退則技術面轉強也要保守。
- 籌碼面：外資/投信「連續買賣超天數」（連續買超是主力吃貨、連續賣超是出貨）、借券賣出餘額趨勢（增加＝空方加碼、減少＝空方回補偏多）。
- 消息面：依提供的新聞標題研判利多利空，但不可捏造。
- 前一日美股：台股常跟隨前夜美股與費半，請把「市場與總經背景」中的美股偏向納入明日進出場節奏（偏空保守、偏多積極）。

建議格式（每支股票）：
操作建議：買進/加碼/持有/減碼/賣出
原因：（50字以內，聚焦最關鍵的 2-3 個指標）
漲跌原因研判：（務必先解釋近期為何漲/跌——結合技術面、法人動向，以及我提供的「市場與總經背景」「近期新聞標題」。例如是大盤/費半連動、個股題材發酵、或公司利空。若資料不足以判斷，就誠實說「依現有資料無法確認原因」，絕不可捏造新聞或事件。）
影響評估：（判斷這是「短期波動」還是「長期/結構性問題」。若只是大盤連動或短期情緒、公司基本面未壞且虧損有限，傾向「續抱撐過」並給觀察點；只有研判為結構性轉壞才建議停損出場。）
關鍵支撐：XX 元
關鍵壓力：XX 元
停利/防守價：XX 元（持倉跌破此價才考慮減碼或出場；獲利部位的移動停利依據）
預估明日區間：+X% ~ +X%（或 -X% ~ -X%）
風險提示：（一句話）

判斷原則：
- 月線扣抵向上 + 乖離 < 6% + 量能健康 = 起漲條件
- 法人連買 + KD 低檔黃金交叉 = 強烈買訊
- 當沖比 > 50% + 爆量上影線 = 籌碼混亂，謹慎
- 跌破 MA20 且月線下彎 = 停損或減碼
- 持倉仍獲利且趨勢未轉弱（站穩 MA5/MA20、月線未下彎）：即使技術面過熱，也以「續抱 + 移動停利」為主，給明確的停利觸發價（例如跌破 MA5 或前低才停利），不要無條件叫賣或減碼，以免賣在起漲段
- 只有「過熱且開始轉弱」（跌破 MA5/MA20、月線下彎、爆量收黑）才建議減碼或停利出場

部位可執行性（重要）：
- 務必依「部位指示」給可執行的建議。持股只有 1-2 張或零股時，無法分批，請在「全部續抱」與「全部出清」之間二擇一，絕對不要建議「減碼一半」「減 1/3」這種做不到的動作。
- 只有部位夠大（3 張以上）才可以講分批減碼，並請給明確張數或比例。

經驗檢討（若有提供「上次建議回顧」）：
- 請先用一句話誠實檢討上次判斷對不對（例如：上次叫減碼後卻又漲了 8%，代表當時對過熱的判斷過早），再給今天的建議；別重複犯同樣的錯。

回應使用繁體中文，語氣直接果斷，不要模糊建議，全程不要使用任何 emoji 或表情符號。"""


_AGENT_ETF_SYSTEM_PROMPT = """你是一位專業的 ETF 投資顧問，負責協助使用者判斷手中或關注的 ETF 是否續抱、加碼或減碼。

重要：ETF 不是個股，請勿用個股的本益比、單一公司基本面、當沖比、融資融券等籌碼指標來下結論。ETF 的買賣判斷應聚焦於：
1. 淨值與市價：目前市價相對淨值是溢價還是折價，溢價過高（常見 >2~3%）追高風險大，折價或貼近淨值較合理。
2. 標的內涵：依 ETF 名稱與類別判斷它追蹤什麼（市值型/高股息/債券/產業主題/槓桿反向），說明其主要成分股或產業曝險的概況與當前處境。
3. 配息與績效：殖利率是否符合訴求（高股息型尤其重要）、近 1/3/6 個月與今年以來的績效表現、是否能穩定填息。
4. 總體環境：結合當前國際局勢、利率與景氣循環、匯率、資金流向等利多利空，評估該類資產的中長期方向。
5. 費用率：內扣費用是否合理，長期持有的成本侵蝕。

你可以參考技術面的支撐壓力作為「進出場時機」的輔助，但不可作為是否持有 ETF 的主要理由——ETF 偏中長期持有，頻繁進出多半不利。

輸出格式：
操作建議：續抱/加碼/分批加碼/持有觀望/部分減碼/賣出
漲跌原因研判：（先解釋近期為何漲/跌——結合我提供的「市場與總經背景」「近期新聞標題」與該 ETF 追蹤標的。多數 ETF 跌是大盤/類股連動而非單一事件；若資料不足就誠實說明，不要捏造。判斷是短期波動還是趨勢轉壞，短期波動且屬中長期配置者傾向續抱。）
ETF 定位：（這檔追蹤什麼、屬於哪種類型、適合什麼樣的投資目的）
淨值與溢價：（市價 vs 淨值，目前是否適合進場的價位）
配息與績效：（殖利率、近期績效、填息能力的評估）
總經與展望：（結合目前國際局勢與該類資產的中長期方向）
理由與建議：（綜合以上，給出明確、可執行的建議與合理的加減碼價位區間）
風險提示：（一句話）

部位可執行性（重要）：依「部位指示」給可執行的建議。持股只有 1-2 張或零股時無法分批，請在「續抱」與「全部出清」之間二擇一，不要建議「部分減碼」「減 1/3」這類做不到的動作；部位夠大才談分批。

經驗檢討：若有提供「上次建議回顧」，請先用一句話檢討上次判斷對不對，再給今天的建議。

若部分資料（如完整持股明細、經理人主動報酬）無法取得，請誠實說明並以可得資料與你對該 ETF 的認識來判斷，不要捏造數據。
回應使用繁體中文，語氣直接務實，全程不要使用任何 emoji 或表情符號。"""


def _agent_system_prompt(data: dict) -> str:
    """依標的型態挑系統提示：ETF 用 ETF 顧問口吻，個股用原本的技術派助理。"""
    return _AGENT_ETF_SYSTEM_PROMPT if (data or {}).get('is_etf') else _AGENT_SYSTEM_PROMPT


def _load_agent_cfg() -> dict:
    with _agent_lock:
        if not os.path.exists(AGENT_FILE):
            return _default_agent_cfg()
        try:
            with open(AGENT_FILE) as f:
                return json.load(f)
        except Exception:
            return _default_agent_cfg()


def _save_agent_cfg(cfg: dict):
    with _agent_lock:
        with open(AGENT_FILE, 'w') as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)


def _agent_api_key(cfg: dict = None) -> str:
    """取得 Claude API Key：優先環境變數（避免金鑰寫死在設定檔／進版控），其次設定檔。"""
    key = os.environ.get('ANTHROPIC_API_KEY') or os.environ.get('CLAUDE_API_KEY')
    if key:
        return key.strip()
    if cfg is None:
        cfg = _load_agent_cfg()
    return (cfg.get('claude_api_key') or '').strip()


def _default_agent_cfg() -> dict:
    return {
        'enabled': False,
        'claude_api_key': '',
        'line_token': '',
        'line_user_id': '',
        'scan_price_min': 10,
        'scan_price_max': 200,
        'scan_min_score': 3,
        'scan_universe': 'top100',
        'holdings': [],          # [{code, name, buy_price, shares, date}]
        'candidates': [],        # last scan results
        'notifications': [],     # last 50 notification logs
        'rec_history': [],       # 建議經驗庫 [{date, code, name, price, pnl_pct, action}]，供回顧檢討
        'today_analysis': {},    # 當日持倉分析快取 {date, results}，收盤總結與目標檢討共用
        'intraday_snapshot_move_pct': 3.0,  # 盤中持倉波動逾此 % 即推快照
        'goal': {                # 目標導向操盤：在期限內把資產累積到目標金額
            'enabled':       False,
            'start_capital': 0,      # 計畫起始/可投入總資金
            'cash':          0,      # 目前未投入的現金（可用來加碼）
            'target_amount': 0,      # 目標金額
            'start_date':    '',     # YYYY-MM-DD
            'target_date':   '',     # YYYY-MM-DD
            'risk':          'balanced',  # conservative / balanced / aggressive
            'last_review':   '',     # 最近一次目標檢討日期
            'last_status':   None,   # 最近一次達標試算快照
        },
        'workbench': {               # 策略工作台：存 AI 策略供每日收盤自動掃描
            'enabled':       False,
            'strategy_name': '',
            'signals':       [],     # 進場訊號代碼清單
            'conditions':    [],     # 籌碼/量能篩選條件 [{type, params}]
            'exit':          {},     # 出場策略 dict
            'universe':      'top100',
            'price_min':     0,
            'price_max':     0,      # 0 視為不限
            'last_run':      '',     # 最近一次自動掃描日期
            'last_result':   None,   # 最近一次掃描＋回測結果快照
        },
        'last_scan': '',
        'last_morning': '',
        'last_close': '',
        'last_close_reminder': '',   # 最近一次「請登錄今日買賣」收盤提醒日期
    }


def _score_breakout(data: dict) -> int:
    """Return multi-factor total score (0-24). Uses mf_score if available."""
    if data.get('error'):
        return 0
    mf = data.get('mf_score')
    if mf:
        return mf.get('total', 0)
    # Fallback: legacy 5-factor
    score = 0
    if data.get('mv5', 0) > data.get('mv20', 1): score += 1
    bias = data.get('bias20', 99)
    if 0 < bias < 6: score += 1
    if 0 < data.get('rebound_pct', 99) < 15: score += 1
    dtr = data.get('day_trade_ratio')
    if dtr is None or dtr < 40: score += 1
    if data.get('price', 0) > data.get('ma20_deduct_price', 99999): score += 1
    return score


def _holding_action_signal(data: dict) -> str:
    """以技術指標規則（非 AI 文字）判定持倉急迫度，回傳 '賣出' / '減碼' / ''。

    盤中去重與警示觸發改用這個決定性訊號，避免因 AI 用字在「減碼/持有觀望」
    之間浮動，導致同一檔在 urgent 名單進進出出而被重複推播。"""
    if not data or data.get('error'):
        return ''
    price = safe_float(data.get('price', 0))
    ma20  = safe_float(data.get('ma20', 0))

    # ETF 偏中長期持有，常態貼著上軌跑（尤其高息型），不適合用個股的短線過熱／跌破月線
    # 來觸發進出。門檻明顯放寬，且不做「跌破月線即賣出」的停損判斷，避免常態誤報。
    ma5 = safe_float(data.get('ma5', 0))
    # 「轉弱」判斷：跌破 5 日線代表短線動能斷掉。賺錢又沿均線上攻的部位，純過熱
    # （RSI 高、乖離大、創新高）不該觸發減碼警示——那只是強勢，該做的是設移動停利
    # 續抱，而不是賣在起漲段。唯有過熱「且」開始轉弱才升級為減碼訊號。
    weakening = ma5 > 0 and price > 0 and price < ma5

    if data.get('is_etf'):
        hot = 0
        if safe_float(data.get('bias20', 0)) >= 15:  hot += 1
        if safe_float(data.get('rsi', 0))    >= 82:  hot += 1
        if safe_float(data.get('bb_pct', 0)) >= 110: hot += 1
        if safe_float(data.get('w52_pct', 0)) >= 99: hot += 1
        return '減碼' if (hot >= 3 and weakening) else ''

    # 個股：賣出（停損）— 跌破月線且月線下彎
    if price > 0 and ma20 > 0 and price < ma20 and '下彎' in str(data.get('ma20_trend', '')):
        return '賣出'
    # 個股：減碼 — 過熱「且」短線轉弱（跌破 MA5）才算；純過熱仍在漲不視為出場訊號。
    hot = 0
    if safe_float(data.get('bias20', 0)) >= 10:  hot += 1
    if safe_float(data.get('rsi', 0))    >= 75:  hot += 1
    if safe_float(data.get('bb_pct', 0)) >= 100: hot += 1
    if safe_float(data.get('k', 0))      >= 85:  hot += 1
    if safe_float(data.get('w52_pct', 0)) >= 95: hot += 1
    return '減碼' if (hot >= 2 and weakening) else ''


def _position_size_directive(shares) -> str:
    """依持股張數，提示 AI 給出「可執行」的建議。零股／1-2 張根本無法分批減碼，
    這種情況只能二擇一（全抱或全出），不要再講減一半、減 1/3。"""
    lots = safe_float(shares) / 1000
    if lots <= 0:
        return "（部位指示：未填持股數，請給方向性建議即可，不要假設可分批。）"
    if lots < 1:
        return (f"（部位指示：僅 {shares} 股零股、不足一張，無法分批。若要調節只能「全部出清」，"
                f"請勿建議減碼一半或減 1/3。）")
    if lots <= 2:
        return (f"（部位指示：僅約 {lots:.0f} 張，無法有意義地分批。請在「全部續抱」與「全部出清」"
                f"之間二擇一，不要建議減碼一半／減 1/3 這類無法執行的動作。）")
    return f"（部位指示：約 {lots:.0f} 張，可分批調節，減碼建議請給明確張數或比例。）"


def _parse_action(rec_text: str) -> str:
    """從 AI 建議文字抓「操作建議：」那一行的動作，存進回顧紀錄用。"""
    for line in (rec_text or '').splitlines():
        if '操作建議' in line:
            val = line.split('：', 1)[-1].split(':', 1)[-1].strip()
            return val[:20] if val else ''
    return ''


def _recent_rec_for(cfg: dict, code: str, today: str) -> dict:
    """取某檔在「今天之前」最近一次的建議紀錄，供 AI 檢討上次判斷準不準。"""
    best = None
    for h in cfg.get('rec_history', []):
        if h.get('code') == code and h.get('date', '') < today:
            if best is None or h['date'] > best['date']:
                best = h
    return best


def _realized_summary_for(cfg: dict, code: str) -> dict:
    """彙總某檔過去的已實現損益（賣出帳本），供 AI 建議引用「這檔你已實現 +X%」。"""
    rows = [s for s in cfg.get('sells', []) if s.get('code') == code]
    if not rows:
        return {}
    pnl  = round(sum(safe_float(r.get('realized_pnl', 0)) for r in rows), 0)
    cost = sum(safe_float(r.get('cost_basis', 0)) for r in rows)
    pct  = round(pnl / cost * 100, 1) if cost else 0
    last = max(rows, key=lambda r: r.get('date', ''))
    return {'pnl': pnl, 'pct': pct, 'count': len(rows), 'last_date': last.get('date', '')}


def _record_recommendations(cfg: dict, today: str, results: list):
    """把今天每檔的建議（動作＋當時價）寫進 rec_history，形成可回顧的經驗庫。
    同一檔同一天只留一筆；總量上限 300 筆。"""
    hist = [h for h in cfg.get('rec_history', []) if not (h.get('date') == today)]
    for r in results:
        action = _parse_action(r.get('recommendation', ''))
        if not action:
            continue
        hist.append({
            'date':   today,
            'code':   r.get('code', ''),
            'name':   r.get('name', ''),
            'price':  r.get('price', 0),
            'pnl_pct': r.get('pnl_pct', 0),
            'action': action,
        })
    cfg['rec_history'] = hist[-300:]


def _etf_recommendation_text(data: dict, holding: dict = None) -> str:
    """Build ETF-specific context for Claude（淨值/溢價/配息/績效/總經為主，技術面只作輔助）。"""
    lines = []
    code  = data.get('code', '')
    name  = data.get('name', code)
    price = data.get('price', 0)
    etf   = data.get('etf', {}) or {}

    lines.append(f"ETF：{name}（{code}）  目前市價：{price} 元")
    if etf.get('category'): lines.append(f"類別：{etf['category']}")
    if etf.get('family'):   lines.append(f"發行：{etf['family']}")

    nav = etf.get('nav')
    if nav:
        prem = etf.get('premium_pct')
        prem_txt = f"（{'溢價' if (prem or 0) >= 0 else '折價'} {abs(prem):.2f}%）" if prem is not None else ''
        lines.append(f"淨值(NAV)：{nav} 元{prem_txt}")
    else:
        lines.append("淨值(NAV)：未取得（請依市價與你對該 ETF 的認識判斷溢折價）")

    dy = etf.get('yield')
    lines.append(f"殖利率：{f'{dy:.2f}%' if dy else '未取得'}")
    if etf.get('expense') is not None:
        lines.append(f"內扣費用率：約 {etf['expense']:.3f}%")

    perf = []
    if etf.get('ret_1m') is not None: perf.append(f"近1月 {etf['ret_1m']:+.2f}%")
    if etf.get('ret_3m') is not None: perf.append(f"近3月 {etf['ret_3m']:+.2f}%")
    if etf.get('ret_6m') is not None: perf.append(f"近6月 {etf['ret_6m']:+.2f}%")
    if etf.get('ytd_return') is not None: perf.append(f"今年以來 {etf['ytd_return']:+.2f}%")
    if perf:
        lines.append("績效：" + "、".join(perf))

    if holding:
        buy_p  = holding.get('buy_price', 0)
        shares = holding.get('shares', 0)
        pnl    = round((price - buy_p) / buy_p * 100, 2) if buy_p else 0
        lots   = safe_float(shares) / 1000
        lines.append(f"持倉資訊：買入均價 {buy_p} 元 / {shares} 股（約 {lots:.1f} 張）/ 未實現損益 {pnl:+.2f}%")
        lines.append(_position_size_directive(shares))

    # 技術面僅供進出場時機輔助（明確標註，避免被當成 ETF 去留的主要依據）
    lines.append(f"\n（技術面僅供進出場時機參考，勿作為 ETF 去留主因）")
    lines.append(f"季線(MA60)：{data.get('ma60','N/A')}  半年高低：{data.get('w52_high','N/A')}/{data.get('w52_low','N/A')}（52週位置 {data.get('w52_pct','N/A')}%）")
    lines.append(f"RSI：{data.get('rsi','N/A')}  KD：K={data.get('k','N/A')} D={data.get('d','N/A')}")

    note = ('\n備註：完整持股明細與經理人主動報酬未由系統提供，請依 ETF 名稱、類別與你對其追蹤標的的認識，'
            '說明主要成分／產業曝險與當前國際局勢、利率景氣對該類資產的影響。')
    lines.append(note)
    return '\n'.join(lines)


def _market_macro_context() -> str:
    """市場與總經背景：沿用既有的隔夜美股快照（費半/那指/標普/道瓊/台積電ADR＋對台股偏向）
    並補上台股加權指數，讓 AI 判斷個股漲跌是否屬大盤/總經連動。快取 30 分鐘、全標的共用。"""
    cached = _cache_get('macro_ctx')
    if cached is not None:
        return cached
    lines = []
    try:
        snap = _us_overnight_snapshot()
        d = snap.get('data', {})
        parts = [f"{d[k]['label']} {d[k]['v']:,.2f}（{d[k]['pct']:+.2f}%）"
                 for k in ('sox', 'nasdaq', 'sp500', 'dow', 'tsm_adr') if k in d]
        if parts:
            lines.append('隔夜美股（最近一交易日收盤）：' + '、'.join(parts)
                         + f"（對台股偏向：{snap.get('bias', '中性')}）")
        if 'usdtwd' in d:
            lines.append(f"美元/台幣：{d['usdtwd']['v']:.2f}（{d['usdtwd']['pct']:+.2f}%）")
    except Exception:
        pass
    try:
        h = yf.Ticker('^TWII').history(period='5d', interval='1d')
        if len(h) >= 2:
            last = float(h['Close'].iloc[-1]); prev = float(h['Close'].iloc[-2])
            lines.append(f"台股加權指數（前一交易日收）：{last:,.0f}（{(last/prev-1)*100:+.2f}%）")
    except Exception:
        pass
    out = ('\n【市場與總經背景】\n' + '\n'.join(lines)) if lines else ''
    _cache_set('macro_ctx', out, ttl=1800)
    return out


def _stock_news_context(code: str, name: str, n: int = 6) -> str:
    """抓個股近期 zh-TW 新聞標題（Google News RSS），餵 AI 研判下跌原因/是否黑天鵝/
    是否長期影響。逐檔快取 2 小時。"""
    if not code:
        return ''
    ck = f'news_{code}'
    cached = _cache_get(ck)
    if cached is not None:
        return cached
    heads = []
    try:
        import urllib.request, urllib.parse
        q = urllib.parse.quote(f'{name} {code}'.strip())
        url = (f'https://news.google.com/rss/search?q={q}'
               f'&hl=zh-TW&gl=TW&ceid=TW:zh-Hant')
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        root = ET.fromstring(urllib.request.urlopen(req, timeout=10).read())
        for it in root.iter('item'):
            title = (it.findtext('title') or '').strip()
            if title and title not in heads:
                heads.append(title)
            if len(heads) >= n:
                break
    except Exception as e:
        print(f'[News] {code}: {e}')
    out = ('\n【近期相關新聞標題（Google News，僅供研判催化劑）】\n'
           + '\n'.join(f'- {h}' for h in heads[:n])) if heads else ''
    _cache_set(ck, out, ttl=7200)
    return out


def _macro_news_block(data: dict) -> str:
    """市場總經背景 + 個股新聞，附在個股分析 context 後面供 AI 研判漲跌原因。"""
    code = (data or {}).get('code', '')
    name = (data or {}).get('name', code)
    return _market_macro_context() + _stock_news_context(code, name)


def _agent_recommendation_text(data: dict, holding: dict = None) -> str:
    """Build context text for Claude agent recommendation."""
    if data.get('is_etf'):
        return _etf_recommendation_text(data, holding) + _macro_news_block(data)

    lines = []
    code  = data.get('code', '')
    name  = data.get('name', code)
    price = data.get('price', 0)
    score = _score_breakout(data)

    lines.append(f"股票：{name}（{code}）  現價：{price} 元  起漲指標得分：{score}/5")

    if holding:
        buy_p  = holding.get('buy_price', 0)
        shares = holding.get('shares', 0)
        pnl    = round((price - buy_p) / buy_p * 100, 2) if buy_p else 0
        pnl_amt = round((safe_float(price) - safe_float(buy_p)) * safe_float(shares), 0)
        lots   = safe_float(shares) / 1000
        lines.append(f"持倉資訊：買入價 {buy_p} 元 / {shares} 股（約 {lots:.1f} 張）/ "
                     f"未實現損益 {pnl_amt:+,.0f} 元（報酬率 {pnl:+.2f}%）")
        lines.append(_position_size_directive(shares))

    lines.append(f"月線方向：{data.get('ma20_trend', 'N/A')}（扣抵估價 {data.get('ma20_deduct_price', 'N/A')}）")
    lines.append(f"乖離率：{data.get('bias20', 'N/A')}%  反彈幅度：{data.get('rebound_pct', 'N/A'):.1f}%")
    lines.append(f"量能：{data.get('vol_structure', 'N/A')}（5MV={data.get('mv5',0):,} / 20MV={data.get('mv20',0):,}）")

    dtr = data.get('day_trade_ratio')
    lines.append(f"當沖比：{f'{dtr}%' if dtr is not None else '未取得'}")

    lines.append(f"KD：K={data.get('k','N/A')} D={data.get('d','N/A')} → {data.get('kd_status','N/A')}")
    lines.append(f"週KD：週K={data.get('week_k','N/A')} 週D={data.get('week_d','N/A')}")
    lines.append(f"RSI：{data.get('rsi','N/A')}")
    lines.append(f"MACD OSC：{data.get('osc','N/A')} → {data.get('osc_trend','N/A')}")
    lines.append(f"布林通道位置：{data.get('bb_pct','N/A')}%  52週位置：{data.get('w52_pct','N/A')}%")
    lines.append(f"均線多頭排列：{'是(MA5>MA20>MA60)' if data.get('ma_bull') else '否'}  量比：{data.get('vol_ratio','N/A')}x  最新K棒：{'收紅' if data.get('last_candle')=='red' else '收黑'}")
    lines.append(f"近5日最高：{data.get('high_5d','N/A')}  近5日最低：{data.get('low_5d','N/A')}")
    lines.append(f"MA5={data.get('ma5','N/A')}  MA20={data.get('ma20','N/A')}  MA60={data.get('ma60','N/A')}")

    inst = data.get('inst')
    if inst:
        total = inst.get('total_net', 0)
        lines.append(f"三大法人：外資 {inst.get('foreign_net',0)/1000:+,.0f} / 投信 {inst.get('trust_net',0)/1000:+,.0f} / 自營 {inst.get('dealer_net',0)/1000:+,.0f} / 合計 {total/1000:+,.0f} 張")

    mg = data.get('margin')
    if mg:
        lines.append(f"融資餘額：{mg.get('margin_today',0):,.0f}（{mg.get('margin_chg',0):+,.0f}）  融券：{mg.get('short_today',0):,.0f}（{mg.get('short_chg',0):+,.0f}）")

    # 借券賣出餘額趨勢（空方動向）
    ld = data.get('lending')
    if ld:
        bal  = safe_float(ld.get('lending_balance', 0))
        prev = safe_float(ld.get('lending_balance_prev', 0))
        chg  = bal - prev
        trend = '增加（空方加碼）' if chg > 0 else ('減少（空方回補）' if chg < 0 else '持平')
        lines.append(f"借券賣出餘額：{bal:,.0f} 股（較前日{trend} {abs(chg):,.0f}）")

    # 法人連續買賣超天數（籌碼連續性）
    ih = data.get('inst_hist')
    if ih and ih.get('foreign'):
        def _streak(seq):
            seq = seq or []
            if not seq:
                return 0
            sign = 1 if seq[0] > 0 else (-1 if seq[0] < 0 else 0)
            if sign == 0:
                return 0
            n = 0
            for v in seq:
                if (v > 0 and sign > 0) or (v < 0 and sign < 0):
                    n += 1
                else:
                    break
            return n * sign
        fs = _streak(ih.get('foreign'))
        ts = _streak(ih.get('trust'))
        def _desc(n):
            if n > 0:  return f'連{n}日買超'
            if n < 0:  return f'連{abs(n)}日賣超'
            return '無連續'
        f_today = (ih.get('foreign') or [0])[0] / 1000
        t_today = (ih.get('trust') or [0])[0] / 1000
        lines.append(f"法人連續：外資 {_desc(fs)}（今 {f_today:+,.0f} 張）/ 投信 {_desc(ts)}（今 {t_today:+,.0f} 張）")

    # 月營收 YoY/MoM（基本面成長動能）
    mr = data.get('month_rev')
    if mr:
        rev_yi = safe_float(mr.get('revenue', 0)) / 1e8   # 億元
        yoy = mr.get('yoy'); mom = mr.get('mom')
        yoy_s = f"YoY {yoy:+.1f}%" if yoy is not None else "YoY 未取得"
        mom_s = f"MoM {mom:+.1f}%" if mom is not None else "MoM 未取得"
        lines.append(f"月營收（{mr.get('month','')}）：{rev_yi:,.2f} 億元（{yoy_s}／{mom_s}）")

    # EPS／最新季度獲利（基本面）
    ep = data.get('eps')
    if ep:
        eps_v = safe_float(ep.get('eps', 0))
        ni = ep.get('net_income')
        ni_s = f"、稅後淨利 {safe_float(ni)/1e8:,.2f} 億元" if ni else ''
        lines.append(f"最新季度 EPS（{ep.get('quarter','')}）：{eps_v:.2f} 元{ni_s}")

    # 多因子評分摘要
    mf = data.get('mf_score') or {}
    if mf:
        grade   = mf.get('grade', '?')
        total_s = mf.get('total', 0)
        max_s   = mf.get('max', 24)
        pct     = mf.get('pct', 0)
        passed  = [f['name'] for f in mf.get('breakdown', []) if f['pass']]
        failed  = [f['name'] for f in mf.get('breakdown', []) if not f['pass']]
        lines.append(f"\n15 因子評分：{total_s}/{max_s}分（{pct}%）等級 {grade}")
        lines.append(f"通過：{'、'.join(passed) if passed else '無'}")
        lines.append(f"未通過：{'、'.join(failed) if failed else '無'}")

    return '\n'.join(lines) + _macro_news_block(data)


def _get_tw_universe(size: str = 'top100') -> list:
    """Return a list of TW stock codes to scan."""
    import urllib.request
    try:
        url = 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        data = json.loads(urllib.request.urlopen(req, timeout=15).read())
        stocks = []
        for item in data:
            code = item.get('Code', '')
            if not code or not code.isdigit() or len(code) != 4:
                continue
            vol = safe_float(str(item.get('TradeVolume', '0')).replace(',', ''))
            price = safe_float(str(item.get('ClosingPrice', '0')).replace(',', ''))
            if vol > 0 and price > 0:
                stocks.append((code, vol, price))
        stocks.sort(key=lambda x: -x[1])
        limit = 200 if size == 'top200' else (50 if size == 'top50' else 100)
        codes = [s[0] for s in stocks[:limit]]
        if codes:
            return codes
        return _fallback_tw_universe(size)
    except Exception as e:
        print(f'[Agent] universe error: {e}（改用內建精選股池）')
        return _fallback_tw_universe(size)


def _fallback_tw_universe(size: str = 'top100') -> list:
    """TWSE openapi 取不到時（本機 IP 常被擋）改用篩選器的精選股池清單。
    個股優先、ETF 殿後：原本依產業字典順序取前 N，開頭 29 檔全是 ETF，top100
    會把金融/傳產/航運/生技等熱門個股整段砍掉（使用者誤以為「熱門股都篩不到」）。
    改成先排個股、ETF 放最後，確保 top100 涵蓋各產業龍頭個股。"""
    seen, indiv, etf = set(), [], []
    for sector, lst in TW_SCREENER_UNIVERSE.items():
        bucket = etf if 'ETF' in sector else indiv
        for c in lst:
            if c not in seen:
                seen.add(c)
                bucket.append(c)
    ordered = indiv + etf
    limit = 200 if size == 'top200' else (50 if size == 'top50' else 100)
    return ordered[:limit]


def _run_agent_scan(cfg: dict) -> list:
    """Scan universe for breakout candidates. Returns list of dicts."""
    price_min = cfg.get('scan_price_min', 10)
    price_max = cfg.get('scan_price_max', 200)
    min_score = cfg.get('scan_min_score', 3)   # now interpreted as min_pct/4 equivalent grade
    universe  = _get_tw_universe(cfg.get('scan_universe', 'top100'))

    results = []

    # Map legacy min_score (2-4) to minimum grade
    grade_map = {2: 'D', 3: 'C', 4: 'B'}
    grade_order = {'A': 4, 'B': 3, 'C': 2, 'D': 1, 'F': 0}
    min_grade = grade_map.get(int(min_score), 'C')
    min_grade_val = grade_order[min_grade]

    def _check_one(code):
        try:
            data = _fetch_predict_data(code)
            if data.get('error'):
                return None
            price = data.get('price', 0)
            if not (price_min <= price <= price_max):
                return None
            mf = data.get('mf_score') or {}
            grade = mf.get('grade', 'F')
            if grade_order.get(grade, 0) < min_grade_val:
                return None
            passed_names = [f['name'] for f in mf.get('breakdown', []) if f['pass']]
            failed_names = [f['name'] for f in mf.get('breakdown', []) if not f['pass']]
            return {
                'code':          code,
                'name':          data.get('name', code),
                'price':         price,
                'score':         mf.get('total', 0),
                'max_score':     mf.get('max', 24),
                'grade':         grade,
                'grade_pct':     mf.get('pct', 0),
                'bias20':        data.get('bias20'),
                'rebound':       data.get('rebound_pct'),
                'kd':            f"K{data.get('k','?')} D{data.get('d','?')}",
                'week_kd':       f"週K{data.get('week_k','?')}",
                'osc':           data.get('osc'),
                'rsi':           data.get('rsi'),
                'bb_pct':        data.get('bb_pct'),
                'w52_pct':       data.get('w52_pct'),
                'ma_bull':       data.get('ma_bull', False),
                'ma20_trend':    data.get('ma20_trend', ''),
                'vol_structure': data.get('vol_structure', ''),
                'vol_ratio':     data.get('vol_ratio', 1.0),
                'last_candle':   data.get('last_candle', ''),
                'inst_total':    data.get('inst', {}).get('total_net') if data.get('inst') else None,
                'passed':        passed_names,
                'failed':        failed_names,
            }
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(_check_one, code): code for code in universe}
        for fut in as_completed(futures):
            r = fut.result()
            if r:
                results.append(r)

    results.sort(key=lambda x: (-grade_order.get(x['grade'], 0), -x['score']))
    return results[:20]


def _news_digest(query: str, n: int = 3) -> str:
    """取近 n 則新聞標題，組成精簡文字供 AI 判斷消息面。"""
    try:
        news = _fetch_gnews(query, max_results=n)
        if not news:
            return ''
        lines = [f"- {a['title']}（{a.get('publisher','')}）" for a in news[:n]]
        return '\n'.join(lines)
    except Exception:
        return ''


def _run_agent_holdings_analysis(cfg: dict, claude_client) -> list:
    """Analyze all holdings and return recommendation list（並行，原本逐檔序列在持倉多時極慢）。

    個別關閉 AI 分析（ai_enabled=False）的持倉直接略過，不抓資料、不呼叫 Opus，
    省 token；晨報/盤中/盤後統合都吃這份過濾後的清單。"""
    holdings = [h for h in cfg.get('holdings', [])
                if h.get('code', '').strip() and h.get('ai_enabled', True)]
    if not holdings:
        return []

    today = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d')

    def _analyze_one(h):
        code = h.get('code', '').strip().upper()
        data = _fetch_predict_data(code)
        ctx  = _agent_recommendation_text(data, h)   # 已含市場總經背景＋個股新聞
        # 經驗回顧：附上「上次建議＋至今實際漲跌」，要求 AI 先檢討再給今天建議
        prior = _recent_rec_for(cfg, code, today)
        cur_p = safe_float(data.get('price', 0))
        if prior and safe_float(prior.get('price', 0)) > 0:
            realized = (cur_p - prior['price']) / prior['price'] * 100
            ctx += (f"\n\n### 【上次建議回顧】\n{prior['date']} 你曾建議「{prior['action']}」，"
                    f"當時股價 {prior['price']} 元；目前 {cur_p} 元（自上次至今 {realized:+.1f}%）。"
                    f"請先用一句話檢討上次判斷是否正確（例如建議減碼後卻續漲、或建議續抱後下跌），"
                    f"再據此修正並給出今天的建議。")
        # 已實現損益：使用者先前若已分批賣出此檔，附上實現損益，建議時可一併考量
        rz = _realized_summary_for(cfg, code)
        if rz:
            ctx += (f"\n\n### 【已實現損益】\n這檔先前已分批賣出 {rz['count']} 次，"
                    f"累計實現損益 {rz['pnl']:+.0f} 元（約 {rz['pct']:+.1f}%，最近一次 {rz['last_date']}）。"
                    f"剩餘部位請延續此脈絡給續抱／停利建議。")
        try:
            resp = claude_client.messages.create(
                model='claude-opus-4-8',
                max_tokens=1100,
                system=_agent_system_prompt(data),
                messages=[{'role': 'user', 'content': f'請針對以下持倉{"ETF" if data.get("is_etf") else "股票"}給出明日操作建議（請一併考量消息面）：\n\n{ctx}'}],
            )
            rec_text = resp.content[0].text
        except Exception as e:
            rec_text = f'分析失敗：{e}'

        buy_p = h.get('buy_price', 0)
        price = data.get('price', 0)
        pnl   = round((price - buy_p) / buy_p * 100, 2) if buy_p else 0
        return {
            'code':       code,
            'name':       data.get('name', code),
            'price':      price,
            'buy_price':  buy_p,
            'pnl_pct':    pnl,
            'score':      _score_breakout(data),
            'action_signal': _holding_action_signal(data),
            'recommendation': rec_text,
        }

    # 並行（含資料抓取＋新聞＋Claude），上限 5 條避免觸發 API 速率限制
    with ThreadPoolExecutor(max_workers=min(5, len(holdings))) as ex:
        results = list(ex.map(_analyze_one, holdings))
    return [r for r in results if r]


def _run_agent_scan_with_ai(cfg: dict, claude_client, us_context: str = '') -> str:
    """Run breakout scan and ask Claude for top picks summary."""
    candidates = _run_agent_scan(cfg)
    if not candidates:
        return '本次掃描未找到符合條件的標的。'

    summary_lines = ['以下為本次掃描結果，請從中挑選最值得關注的 3-5 支，並說明理由：', '']
    if us_context:
        summary_lines.append(us_context + '（台股開盤常跟隨昨夜美股，請把此偏向納入挑選與進場節奏）')
        summary_lines.append('')
    for c in candidates[:10]:
        summary_lines.append(
            f"{c['name']}（{c['code']}） 現價:{c['price']} 得分:{c['score']}/5 "
            f"乖離:{c.get('bias20','?')}% 反彈:{c.get('rebound','?'):.1f}% "
            f"{c.get('kd','')} 月線:{c.get('ma20_trend','')[:4]} "
            f"量能:{c.get('vol_structure','')[:4]}"
        )

    try:
        resp = claude_client.messages.create(
            model='claude-opus-4-8',
            max_tokens=600,
            system=_AGENT_SYSTEM_PROMPT,
            messages=[{'role': 'user', 'content': '\n'.join(summary_lines)}],
        )
        return resp.content[0].text
    except Exception as e:
        return f'AI 分析失敗：{e}'


def _agent_notify(cfg: dict, title: str, body: str):
    """Send LINE notification and log it."""
    token   = cfg.get('line_token', '')
    user_id = cfg.get('line_user_id', '')
    body    = _strip_emoji(body)
    msg     = f'{title}\n{body}'

    if token and user_id:
        _push_line_msg(token, user_id, msg)

    log_entry = {
        'time':  pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M'),
        'title': title,
        'body':  body[:500],
    }
    cfg.setdefault('notifications', []).insert(0, log_entry)
    cfg['notifications'] = cfg['notifications'][:50]
    _save_agent_cfg(cfg)
    print(f'[Agent] notify: {title}')


def _is_market_hours() -> bool:
    now = pd.Timestamp.now(tz='Asia/Taipei')
    if now.weekday() >= 5:
        return False
    t = now.hour * 60 + now.minute
    return 540 <= t <= 810   # 09:00 - 13:30 → 540-810 minutes


def _is_premarket() -> bool:
    now = pd.Timestamp.now(tz='Asia/Taipei')
    if now.weekday() >= 5:
        return False
    t = now.hour * 60 + now.minute
    return 500 <= t < 540   # 08:20 - 09:00


def _is_close_time() -> bool:
    now = pd.Timestamp.now(tz='Asia/Taipei')
    if now.weekday() >= 5:
        return False
    t = now.hour * 60 + now.minute
    return 825 <= t <= 855   # 13:45 - 14:15


def _is_us_market_hours() -> bool:
    """美股常規盤（美東 09:30-16:00），用美東時區判斷以自動處理日光節約。"""
    now = pd.Timestamp.now(tz='America/New_York')
    if now.weekday() >= 5:
        return False
    t = now.hour * 60 + now.minute
    return 570 <= t <= 960   # 09:30 - 16:00


def _ticker_session_open(ticker: str) -> bool:
    """該標的所屬市場目前是否在交易時段（台股 vs 美股），用來決定要不要推播。"""
    tk = (ticker or '').upper()
    if tk.endswith('.TW') or tk.endswith('.TWO'):
        return _is_market_hours()
    return _is_us_market_hours()


# ═══════════════════════════════════════════════════════════════════════════
#  目標導向操盤層（目標金額/期限 → 需要報酬 → 結合持倉與現金 → 每日買賣建議）
# ═══════════════════════════════════════════════════════════════════════════

def _quick_price(code: str) -> float:
    """抓單一台股最新收盤價（上市找不到退回上櫃）。"""
    try:
        h = yf.Ticker(tw_normalize(code)).history(period='5d', interval='1d')
        if h.empty:
            h = yf.Ticker(code + '.TWO').history(period='5d', interval='1d')
        return safe_float(h['Close'].iloc[-1]) if not h.empty else 0.0
    except Exception:
        return 0.0


# 昨夜美股對台股的領先指標（費半/那指/台積電ADR 對半導體權重最大）
_US_INDEX_SET = [
    ('sox',     '^SOX',     '費城半導體'),
    ('nasdaq',  '^IXIC',    '那斯達克'),
    ('sp500',   '^GSPC',    '標普500'),
    ('dow',     '^DJI',     '道瓊'),
    ('vix',     '^VIX',     'VIX'),
    ('tsm_adr', 'TSM',      '台積電ADR'),
    ('usdtwd',  'USDTWD=X', '美元台幣'),
]


def _us_overnight_snapshot() -> dict:
    """抓昨夜美股各指數漲跌，並用科技權重算出對台股今日開盤的偏向（偏多/偏空/中性）。"""
    cached = _cache_get('us_overnight')
    if cached:
        return cached
    data = {}
    for key, sym, label in _US_INDEX_SET:
        try:
            h = yf.Ticker(sym).history(period='5d')
            closes = [c for c in h['Close'].tolist() if c == c]   # 去除 NaN
            if len(closes) >= 2 and closes[-2]:
                data[key] = {'label': label, 'v': round(closes[-1], 2),
                             'pct': round((closes[-1] / closes[-2] - 1) * 100, 2)}
        except Exception:
            pass

    # 科技權重偏向：費半最重，台積電 ADR、那指次之
    weights = {'sox': 0.35, 'nasdaq': 0.25, 'tsm_adr': 0.25, 'sp500': 0.15}
    score = sum(data[k]['pct'] * w for k, w in weights.items() if k in data)
    bias = '偏多' if score > 0.4 else ('偏空' if score < -0.4 else '中性')
    note = '；VIX 跳升，留意避險情緒' if data.get('vix', {}).get('pct', 0) > 8 else ''

    parts = [f"{data[k]['label']} {data[k]['pct']:+.2f}%"
             for k in ('sox', 'nasdaq', 'sp500', 'dow', 'tsm_adr') if k in data]
    summary = f"昨夜美股：{'、'.join(parts)}（對台股偏向：{bias}{note}）" if parts else ''

    out = {'data': data, 'bias': bias, 'score': round(score, 2), 'summary': summary}
    _cache_set('us_overnight', out, ttl=600)
    return out


def _portfolio_snapshot(holdings: list) -> dict:
    """依持倉算成本、目前市值、未實現損益。"""
    rows, cost, mkt = [], 0.0, 0.0
    for h in holdings:
        code   = str(h.get('code', '')).upper().strip()
        if not code:
            continue
        shares = safe_float(h.get('shares', 0))
        buy    = safe_float(h.get('buy_price', 0))
        price  = _quick_price(code)
        v, c   = price * shares, buy * shares
        cost += c; mkt += v
        rows.append({
            'code': code, 'name': h.get('name', code) or code,
            'shares': shares, 'buy_price': buy, 'price': round(price, 2),
            'value': round(v, 0),
            'pnl_pct': round((price - buy) / buy * 100, 2) if buy else 0,
        })
    return {'rows': rows, 'cost': round(cost, 0), 'market_value': round(mkt, 0),
            'pnl': round(mkt - cost, 0),
            'pnl_pct': round((mkt - cost) / cost * 100, 2) if cost else 0}


def _goal_metrics(goal: dict, market_value: float, cash: float = 0.0) -> dict:
    """達標試算：需要的年化報酬、進度、是否落後於目標曲線。"""
    start_cap = safe_float(goal.get('start_capital', 0))
    target    = safe_float(goal.get('target_amount', 0))
    current   = market_value + safe_float(cash)
    try:
        sd = pd.Timestamp(goal.get('start_date'))
        td = pd.Timestamp(goal.get('target_date'))
    except Exception:
        return {'valid': False}
    if pd.isna(sd) or pd.isna(td) or start_cap <= 0 or target <= 0 or td <= sd:
        return {'valid': False}

    today       = pd.Timestamp.now(tz='Asia/Taipei').tz_localize(None).normalize()
    years_total = max((td - sd).days / 365.25, 0.01)
    elapsed     = min(max((today - sd).days / 365.25, 0), years_total)
    years_left  = max(years_total - elapsed, 0.01)

    req_cagr     = (target / start_cap) ** (1 / years_total) - 1
    on_track_val = start_cap * (1 + req_cagr) ** elapsed
    req_cagr_now = (target / current) ** (1 / years_left) - 1 if current > 0 else None

    return {
        'valid':         True,
        'start_capital': round(start_cap, 0),
        'target_amount': round(target, 0),
        'current_value': round(current, 0),
        'years_total':   round(years_total, 2),
        'years_left':    round(years_left, 2),
        'req_cagr':      round(req_cagr * 100, 2),          # 整段期間需要的年化
        'req_cagr_now':  round(req_cagr_now * 100, 2) if req_cagr_now is not None else None,  # 從現在到期限需要的年化
        'on_track_value':round(on_track_val, 0),            # 此刻按計畫應達到的資產
        'gap':           round(current - on_track_val, 0),  # 領先(+)/落後(-)
        'progress_pct':  round(current / target * 100, 1),  # 距目標完成度
    }


def _goal_status(cfg: dict) -> dict:
    """組合『目標 + 持倉快照 + 達標試算』給前端與每日檢討使用。"""
    goal = cfg.get('goal', {}) or {}
    snap = _portfolio_snapshot(cfg.get('holdings', []))
    metrics = _goal_metrics(goal, snap['market_value'], goal.get('cash', 0))
    return {'goal': goal, 'portfolio': snap, 'metrics': metrics}


_GOAL_CHAT_SYSTEM_PROMPT = """你是使用者的「目標導向操盤」討論夥伴與教練。使用者會在這裡跟你討論他的持股、買賣決策與達標進度，並回饋他實際做了哪些買賣，你要陪他一起檢討對錯、一起進步。

每則對話開頭會附上【目前狀況】，裡面有：使用者的目標與達標進度、目前持倉（含未實現損益）、已實現買賣帳本（他過去真的買進賣出的紀錄與賺賠）、以及最近一次 AI 產生的明日買賣計畫。請務必根據這些真實資料回答，不要說「我看不到你的持股」——你看得到，就寫在【目前狀況】裡。

你的任務：
1. 當使用者問「我這檔該不該賣／加碼」時，結合他的成本、損益、達標需要的年化報酬給明確建議（買進／加碼／續抱／減碼／賣出），不要模糊。
2. 當使用者回饋「我昨天賣了X」「我買了Y」時，誠實檢討這個決策事後看對不對：賣得太早還是剛好、買在相對高還是低，並說出下次可以怎麼修正。語氣直接但不責備，重點是一起變強。
3. 隨時把建議扣回「達標」這個總目標：落後計畫曲線時可略積極、領先時可保守落袋。
4. 持倉中的 ETF（代碼多以 00 開頭）以中長期角度（淨值溢折價、配息、總經）判斷，不要因短線技術指標就叫賣。

回應使用繁體中文，語氣直接務實，全程不要使用任何 emoji 或表情符號。"""


def _goal_chat_context(cfg: dict) -> str:
    """組『目前狀況』脈絡：目標進度＋持倉＋已實現買賣帳本＋最近計畫，餵給目標頁 AI 討論。"""
    status  = _goal_status(cfg)
    goal    = status['goal']
    snap    = status['portfolio']
    metrics = status['metrics']
    lines = ['【目前狀況（系統即時提供，請據此回答）】']

    if metrics.get('valid'):
        lines.append(
            f"目標：在 {goal.get('target_date')} 前把資產累積到 {metrics['target_amount']:,.0f} 元"
            f"（起始 {metrics['start_capital']:,.0f} 元，起算日 {goal.get('start_date')}）。")
        lines.append(
            f"目前總資產 {metrics['current_value']:,.0f} 元（持股市值 {snap['market_value']:,.0f} ＋ 現金 "
            f"{safe_float(goal.get('cash', 0)):,.0f}），完成度 {metrics['progress_pct']}%，剩 {metrics['years_left']} 年。")
        gap = metrics['gap']
        lines.append(
            f"達標需要年化報酬 {metrics['req_cagr']}%，從現在到期限還需年化 {metrics['req_cagr_now']}%；"
            f"目前{'領先' if gap >= 0 else '落後'}計畫曲線 {abs(gap):,.0f} 元。")
    else:
        lines.append('（目標參數尚未完整設定，僅就持倉與買賣紀錄討論。）')
    lines.append(f"可投入現金：{safe_float(goal.get('cash', 0)):,.0f} 元；風險偏好：{goal.get('risk', 'balanced')}。")

    lines.append('\n--- 目前持倉（未實現損益）---')
    if snap['rows']:
        for r in snap['rows']:
            rz = _realized_summary_for(cfg, r['code'])
            extra = f"；此檔過去已實現 {rz['pnl']:+,.0f} 元（{rz['pct']:+.1f}%，{rz['count']} 筆）" if rz else ''
            lines.append(
                f"{r['name']}（{r['code']}）{r['shares']:g} 股，成本 {r['buy_price']} 現價 {r['price']}，"
                f"市值 {r['value']:,.0f}，未實現 {r['pnl_pct']:+.1f}%{extra}")
    else:
        lines.append('（目前無持倉）')

    sells = sorted(cfg.get('sells', []), key=lambda s: s.get('date', ''), reverse=True)
    lines.append('\n--- 已實現買賣帳本（使用者真的賣出的紀錄）---')
    if sells:
        total = round(sum(safe_float(s.get('realized_pnl', 0)) for s in sells), 0)
        for s in sells[:12]:
            note = f"，備註：{s['note']}" if s.get('note') else ''
            lines.append(
                f"{s.get('date')} 賣出 {s.get('name')}（{s.get('code')}）{safe_float(s.get('shares',0)):g} 股，"
                f"買 {s.get('buy_price')} → 賣 {s.get('sell_price')}，實現 {safe_float(s.get('realized_pnl',0)):+,.0f} 元"
                f"（{safe_float(s.get('realized_pct',0)):+.1f}%）{note}")
        lines.append(f"累計已實現損益：{total:+,.0f} 元。")
    else:
        lines.append('（目前帳本沒有任何賣出紀錄。提醒：賣出請走 AI Agent 頁的「賣出記帳」，刪除持倉不會留下買賣紀錄，AI 就無法幫你檢討買賣對錯。）')

    plan = (goal.get('last_plan') or '').strip()
    if plan:
        lines.append(f"\n--- 最近一次 AI 明日買賣計畫（{goal.get('last_review','')}）---")
        lines.append(plan[:1200])

    return '\n'.join(lines)


def _run_goal_review(cfg: dict, client) -> dict:
    """今晚操盤總結（統一盤後產出）：使用者登錄完當日買賣後手動觸發。
    結合前一夜美股→持倉去留→買進候選→目標進度，一次 Opus 產生明日操盤計畫並推 LINE。
    同時吸收舊「收盤總結」的職責：把今天建議寫進經驗庫、快取本日持倉分析，
    避免「收盤總結＋目標檢討」兩則疊床架屋重複耗 token。"""
    goal = cfg.get('goal', {}) or {}

    status  = _goal_status(cfg)
    metrics = status['metrics']
    snap    = status['portfolio']

    # 同一天若已算過持倉分析（例如稍早按過一次）就重用，避免重複耗 token；
    # 否則現算一次（會自動略過已關閉 AI 監測 ai_enabled=False 的持倉）。
    today_str = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d')
    cache = cfg.get('today_analysis') or {}
    if cache.get('date') == today_str and cache.get('results'):
        holdings_analysis = cache['results']
        fresh = False
    else:
        holdings_analysis = _run_agent_holdings_analysis(cfg, client)
        fresh = True
    candidates        = _run_agent_scan(cfg)[:8]
    us                = _us_overnight_snapshot()

    # 組給 Claude 的脈絡
    lines = ['你是使用者的目標導向操盤助理。以下是目前狀況，請依「達標」角度給出明確、可執行的明日操作計畫。', '']
    if us.get('summary'):
        lines.append(us['summary'] + '。台股常受前一夜美股影響，請把這個偏向納入明日進出場節奏（偏空時保守、偏多時可積極，半導體類股看費半與台積電ADR）。')
    if metrics.get('valid'):
        lines.append(f"目標：在 {goal.get('target_date')} 前把資產累積到 {metrics['target_amount']:,.0f} 元"
                     f"（起始 {metrics['start_capital']:,.0f} 元）。")
        lines.append(f"目前總資產 {metrics['current_value']:,.0f} 元（持股市值 {snap['market_value']:,.0f} + 現金 {safe_float(goal.get('cash',0)):,.0f}），"
                     f"完成度 {metrics['progress_pct']}%。")
        lines.append(f"達標需要年化報酬 {metrics['req_cagr']}%；從現在到期限還需年化 {metrics['req_cagr_now']}%。")
        gap = metrics['gap']
        lines.append(f"目前{'領先' if gap >= 0 else '落後'}計畫曲線 {abs(gap):,.0f} 元"
                     f"（此刻應達 {metrics['on_track_value']:,.0f} 元）。")
    else:
        lines.append('（目標參數尚未完整設定，僅就持倉與候選給建議。）')
    lines.append(f"可投入現金：{safe_float(goal.get('cash', 0)):,.0f} 元；風險偏好：{goal.get('risk','balanced')}。")

    lines.append('\n--- 目前持倉 ---')
    if holdings_analysis:
        for h in holdings_analysis:
            lines.append(f"{h['name']}（{h['code']}）現價 {h['price']} 損益 {h['pnl_pct']:+.1f}%；分析：{h['recommendation'][:120]}")
    else:
        lines.append('（無持倉）')

    lines.append('\n--- 今日篩出的買進候選（15 因子評分）---')
    if candidates:
        for c in candidates:
            lines.append(f"{c['name']}（{c['code']}）現價 {c['price']} 評等 {c['grade']}（{c['score']}/{c['max_score']}）"
                         f" 乖離 {c.get('bias20','?')}% {c.get('kd','')}")
    else:
        lines.append('（今日無符合條件候選）')

    lines.append(
        '\n請用繁體中文、不要 emoji，輸出以下四段：'
        '\n1) 進度評估：一句話講現在達標機率與該偏積極或保守。'
        '\n2) 明日買進清單：從候選挑 1-3 檔，每檔給「建議投入金額或張數（用可投入現金估算）＋進場價位區間＋停損價」。'
        '\n3) 該賣出/減碼：列出持倉中該獲利了結或停損的，講明理由與價位；沒有就說「持倉續抱」。'
        '\n4) 一句總結。'
        '\n\n注意：持倉中若為 ETF（代碼多以 00 開頭），請以中長期角度（淨值溢折價、配息、總經方向）判斷去留，'
        '不要因為短線技術指標就建議賣出 ETF；個股才適合用技術面做積極進出。'
    )

    try:
        resp = client.messages.create(
            model='claude-opus-4-8', max_tokens=2500,
            system=_AGENT_SYSTEM_PROMPT,
            messages=[{'role': 'user', 'content': '\n'.join(lines)}],
        )
        plan = _strip_emoji(resp.content[0].text)
        if resp.stop_reason == 'max_tokens':
            plan += '\n\n（註：本則建議因長度上限被截斷，可至目標操盤頁重新檢討取得完整內容。）'
    except Exception as e:
        plan = f'AI 檢討失敗：{e}'

    today = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d')
    cfg = _load_agent_cfg()
    cfg.setdefault('goal', {})
    cfg['goal']['last_review']   = today
    cfg['goal']['last_status']   = status
    cfg['goal']['last_plan']     = plan
    cfg['goal']['review_status'] = 'done'
    cfg['candidates'] = candidates
    # 吸收舊收盤總結職責：剛現算的持倉分析才寫經驗庫＋快取，避免同日重按重複記錄
    if fresh:
        _record_recommendations(cfg, today, holdings_analysis)
        cfg['today_analysis'] = {'date': today, 'results': holdings_analysis}
    _save_agent_cfg(cfg)

    head = ''
    if metrics.get('valid'):
        head = (f"完成度 {metrics['progress_pct']}%・還需年化 {metrics['req_cagr_now']}%・"
                f"{'領先' if metrics['gap'] >= 0 else '落後'} {abs(metrics['gap']):,.0f} 元\n")
    if us.get('summary'):
        head += us['summary'] + '\n'
    if head:
        head += '\n'
    _agent_notify(cfg, f'[{today}] 今晚操盤總結', head + plan)
    return {'status': status, 'plan': plan}


def _agent_loop():
    if not _IS_SCHEDULER:
        return
    time.sleep(20)
    print('[Agent] background loop started (this worker is the scheduler)')
    while True:
        try:
            cfg = _load_agent_cfg()
            if not cfg.get('enabled') or not cfg.get('claude_api_key'):
                time.sleep(120)
                continue

            import anthropic
            client = anthropic.Anthropic(api_key=cfg['claude_api_key'])
            now    = pd.Timestamp.now(tz='Asia/Taipei')
            today  = now.strftime('%Y-%m-%d')

            # Morning briefing (08:20-09:00)
            if _is_premarket() and cfg.get('last_morning', '') != today:
                cfg = _load_agent_cfg()
                us = _us_overnight_snapshot()
                holdings_analysis = _run_agent_holdings_analysis(cfg, client)
                scan_ai = _run_agent_scan_with_ai(cfg, client, us.get('summary', ''))

                lines = [f'[{today}] 早盤前分析報告', '']
                if us.get('summary'):
                    lines.append(us['summary'])
                    lines.append('')
                if holdings_analysis:
                    lines.append('--- 持倉建議 ---')
                    for h in holdings_analysis:
                        lines.append(f"{h['name']}（{h['code']}） 現價:{h['price']} 損益:{h['pnl_pct']:+.1f}%")
                        lines.append(h['recommendation'][:200])
                        lines.append('')
                lines.append('--- 今日潛力標的 ---')
                lines.append(scan_ai[:400])

                cfg['last_morning'] = today
                _agent_notify(cfg, '晨報', '\n'.join(lines))

            # Intraday scans (every 30 min during market hours)
            elif _is_market_hours():
                last_scan = cfg.get('last_scan', '')
                last_scan_ts = pd.Timestamp(last_scan, tz='Asia/Taipei') if last_scan else pd.Timestamp('2000-01-01', tz='Asia/Taipei')
                if (now - last_scan_ts).total_seconds() >= 1800:
                    cfg = _load_agent_cfg()
                    holdings_analysis = _run_agent_holdings_analysis(cfg, client)

                    # 急迫度改用決定性技術訊號（_holding_action_signal），不再比對 AI 文字，
                    # 避免同一檔因 AI 用字浮動而在 urgent 名單進出、被重複推播。
                    urgent = [h for h in holdings_analysis if h.get('action_signal') in ('賣出', '減碼')]
                    cur_urgent = {h['code']: h['action_signal'] for h in urgent}
                    # 去重複＋升級偵測：每天重新基準。除了「新出現」的標的，
                    # 訊號等級升高（減碼→賣出）也視為需要重新警示，避免惡化被靜默吃掉。
                    _lv = {'減碼': 1, '賣出': 2}
                    prev = cfg.get('last_intraday_urgent', {}) if cfg.get('last_intraday_date') == today else {}
                    if isinstance(prev, list):   # 相容舊格式（純代碼清單）
                        prev = {c: '減碼' for c in prev}
                    new_codes = [c for c, s in cur_urgent.items()
                                 if _lv.get(s, 0) > _lv.get(prev.get(c, ''), 0)]

                    # 盤中即時快照：即使沒有新急迫訊號，只要有持倉自上次推播後波動達門檻，
                    # 也推一份精簡現價/損益快照，讓盤中內容不至於整天靜默。
                    move_thr  = safe_float(cfg.get('intraday_snapshot_move_pct', 3.0))
                    snap_date = cfg.get('last_snapshot_date')
                    last_snap = cfg.get('last_snapshot_prices', {}) if snap_date == today else {}
                    moved = []
                    for h in holdings_analysis:
                        prevp = safe_float(last_snap.get(h['code'], 0))
                        if prevp > 0 and abs(h['price'] - prevp) / prevp * 100 >= move_thr:
                            moved.append(h)

                    cfg['last_scan']            = now.strftime('%Y-%m-%d %H:%M')
                    cfg['candidates']           = _run_agent_scan(cfg)
                    cfg['last_intraday_urgent'] = cur_urgent
                    cfg['last_intraday_date']   = today

                    if new_codes:
                        urgent_new = [h for h in urgent if h['code'] in new_codes]
                        lines = [f'[{now.strftime("%H:%M")}] 盤中監控',
                                 '!! 注意 !! 以下持倉出現新的賣出/減碼訊號：']
                        for h in urgent_new:
                            esc = '（訊號升級為賣出）' if prev.get(h['code']) == '減碼' and h['action_signal'] == '賣出' else ''
                            lines.append(f"[{h['action_signal']}]{esc} {h['name']}（{h['code']}） 現價:{h['price']} 損益:{h['pnl_pct']:+.1f}%")
                            lines.append(h['recommendation'][:150])
                        _agent_notify(cfg, '盤中監控', '\n'.join(lines))
                        cfg['last_snapshot_prices'] = {h['code']: h['price'] for h in holdings_analysis}
                        cfg['last_snapshot_date']   = today
                        _save_agent_cfg(cfg)
                    elif moved:
                        lines = [f'[{now.strftime("%H:%M")}] 盤中持倉快照',
                                 f'（波動逾 {move_thr:.0f}% 的持倉）']
                        for h in moved:
                            sig = f" 訊號:{h['action_signal']}" if h.get('action_signal') else ''
                            lines.append(f"{h['name']}（{h['code']}） 現價:{h['price']} 損益:{h['pnl_pct']:+.1f}%{sig}")
                        _agent_notify(cfg, '盤中快照', '\n'.join(lines))
                        cfg['last_snapshot_prices'] = {h['code']: h['price'] for h in holdings_analysis}
                        cfg['last_snapshot_date']   = today
                        _save_agent_cfg(cfg)
                    else:
                        # 首次掃描當天先建立快照基準，之後才能比較波動
                        if not last_snap:
                            cfg['last_snapshot_prices'] = {h['code']: h['price'] for h in holdings_analysis}
                            cfg['last_snapshot_date']   = today
                        _save_agent_cfg(cfg)

            # 收盤提醒 (13:45-14:15)：不再自動跑全檔 Opus 收盤總結。
            # 改推一則「請登錄今日買賣」提醒；統合的操盤總結由使用者在「目標操盤」頁
            # 按【產生今晚操盤總結】時，用 _run_goal_review（統合版）一次產出——結合前一夜美股、
            # 持倉去留與目標進度，省 token 且不再「收盤總結＋目標檢討」兩則疊床架屋。
            elif _is_close_time() and cfg.get('last_close_reminder', '') != today:
                cfg = _load_agent_cfg()
                cfg['last_close_reminder'] = today
                _save_agent_cfg(cfg)
                _agent_notify(cfg, '收盤提醒',
                    '今日台股已收盤。請先到「持倉管理」登錄今天的買賣（買進/賣出），'
                    '完成後到「目標操盤」頁按【產生今晚操盤總結】。'
                    '系統會結合前一夜美股走勢與你的持倉、目標，產出明日操盤建議。')

            # Daily strategy optimizer (after 15:00, once per day)
            elif now.hour >= 15 and cfg.get('last_optimize', '') != today:
                try:
                    _run_daily_optimizer(_load_agent_cfg(), client)
                except Exception as e:
                    print(f'[Agent] optimizer error: {e}')

            # 策略工作台每日收盤自動掃描（盤後，每日一次）
            elif (now.hour >= 15
                  and cfg.get('workbench', {}).get('enabled')
                  and cfg.get('workbench', {}).get('signals')
                  and cfg.get('workbench', {}).get('last_run', '') != today):
                try:
                    _run_workbench_daily(_load_agent_cfg(), client)
                except Exception as e:
                    print(f'[Agent] workbench scan error: {e}')

        except Exception as e:
            print(f'[Agent] loop error: {e}')

        time.sleep(60)


threading.Thread(target=_agent_loop, daemon=True).start()


# ── Agent API endpoints ────────────────────────────────────────────────────

@app.route('/agent')
def agent_page():
    return render_template('agent.html')


@app.route('/api/agent/config', methods=['GET', 'POST'])
def agent_config_api():
    if request.method == 'GET':
        cfg = _load_agent_cfg()
        safe = dict(cfg)
        # 機密欄位一律遮罩，避免任何人讀取設定就拿到金鑰／推播 token
        if safe.get('claude_api_key'):
            safe['claude_api_key'] = '***' + safe['claude_api_key'][-4:]
        if safe.get('line_token'):
            safe['line_token'] = '***' + safe['line_token'][-4:]
        if safe.get('finmind_token'):
            safe['finmind_token'] = '***' + safe['finmind_token'][-4:]
        return jsonify(safe)

    body = request.get_json(force=True)
    cfg  = _load_agent_cfg()

    for field in ['enabled', 'line_user_id',
                  'scan_price_min', 'scan_price_max', 'scan_min_score', 'scan_universe']:
        if field in body:
            cfg[field] = body[field]

    # 機密欄位：只有收到「非遮罩」的新值才覆寫，避免把已存的金鑰／token 洗成 ***
    for secret in ['claude_api_key', 'line_token', 'finmind_token']:
        val = body.get(secret)
        if val and not str(val).startswith('***'):
            cfg[secret] = val
            if secret == 'finmind_token':
                _finmind_token_cache['loaded'] = False   # 讓快取重新讀取新 token

    _save_agent_cfg(cfg)
    return jsonify({'ok': True})


@app.route('/api/agent/holdings', methods=['GET', 'POST', 'DELETE'])
def agent_holdings_api():
    cfg = _load_agent_cfg()

    if request.method == 'GET':
        holdings = cfg.get('holdings', [])
        # raw=1：只回原始持倉清單（不打 yfinance 報價），供投資組合頁快速共用同一份資料
        if request.args.get('raw'):
            return jsonify(holdings)
        # 並行抓取各持倉報價（原本逐檔序列，持倉多時首次載入很慢）
        def _fetch_holding_price(code):
            cached = _cache_get(f'agent_h_{code}')
            if cached:
                return code, cached
            try:
                stock = yf.Ticker(code + '.TW')
                info  = stock.info
                cur   = safe_float(info.get('currentPrice', info.get('regularMarketPrice', 0)))
                if cur <= 0:
                    stock = yf.Ticker(code + '.TWO')
                    info  = stock.info
                    cur   = safe_float(info.get('currentPrice', info.get('regularMarketPrice', 0)))
                data = {'price': cur, 'name': tw_cn_name(code, info.get('shortName', code))}
                _cache_set(f'agent_h_{code}', data, ttl=120 if cur else 600)
            except Exception:
                data = {'price': 0, 'name': code}
                _cache_set(f'agent_h_{code}', data, ttl=300)
            return code, data

        codes = [h.get('code', '') for h in holdings]
        price_map = {}
        if codes:
            # 報價是外部 I/O，Yahoo 限流時單檔 .info 可能吊死數十秒，早上開盤尤其明顯。
            # 設整體硬性逾時：逾時未回的標的改用快取、沒快取就先給 0（前端照常顯示持倉，
            # 只是該檔暫無即時報價），確保此 API 一定在數秒內回應、不再「一直載入中」。
            ex = ThreadPoolExecutor(max_workers=min(12, len(codes)))
            futures = {ex.submit(_fetch_holding_price, c): c for c in codes}
            try:
                for fut in as_completed(futures, timeout=8):
                    code, data = fut.result()
                    price_map[code] = data
            except FuturesTimeout:
                pass
            ex.shutdown(wait=False)   # 不等慢速 stragglers，背景跑完即填回快取供下次命中
            for c in codes:
                if c not in price_map:
                    price_map[c] = _cache_get(f'agent_h_{c}') or {'price': 0, 'name': c}

        enriched = []
        for h in holdings:
            code       = h.get('code', '')
            price_data = price_map.get(code, {'price': 0, 'name': code})
            buy_p = h.get('buy_price', 0)
            cur   = price_data.get('price', 0)
            pnl   = round((cur - buy_p) / buy_p * 100, 2) if buy_p and cur else 0
            enriched.append({**h, 'current_price': cur, 'pnl_pct': pnl,
                              'name': price_data.get('name', code)})
        return jsonify(enriched)

    if request.method == 'POST':
        body = request.get_json(force=True)
        code = body.get('code', '').strip().upper().replace('.TW', '').replace('.TWO', '')
        if not code:
            return jsonify({'error': '代碼不可為空'}), 400
        prev     = next((h for h in cfg.get('holdings', []) if h.get('code') == code), {})
        existing = [h for h in cfg.get('holdings', []) if h.get('code') != code]
        # 以既有紀錄為底合併，避免某一邊編輯時把另一邊設定的欄位（如群組）洗掉
        record = dict(prev)
        new_price = safe_float(body.get('buy_price', prev.get('buy_price', 0)))
        new_shares = safe_int(body.get('shares', prev.get('shares', 0)))
        new_date   = body.get('date') or prev.get('date') or pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d')
        # merge=true（單筆加碼）：同代碼已存在時累加股數並算加權平均成本，不覆寫舊紀錄。
        # 編輯持倉與截圖匯入不帶 merge（沿用覆寫，匯入為券商當前快照、合併會重複計算）。
        if body.get('merge') and prev:
            old_shares = safe_int(prev.get('shares', 0))
            old_price  = safe_float(prev.get('buy_price', 0))
            add_shares = safe_int(body.get('shares', 0))
            add_price  = safe_float(body.get('buy_price', 0))
            total = old_shares + add_shares
            if total > 0 and add_shares > 0 and old_shares > 0 and old_price > 0 and add_price > 0:
                new_shares = total
                new_price  = round((old_price * old_shares + add_price * add_shares) / total, 4)
                new_date   = prev.get('date') or new_date   # 保留原始建倉日作為成本基準
        record.update({
            'code':      code,
            'buy_price': new_price,
            'shares':    new_shares,
            'date':      new_date,
        })
        # 只在請求有帶這些欄位時才覆寫，沒帶就沿用既有值
        for field in ('note', 'group', 'name'):
            if field in body:
                record[field] = body.get(field, '')
        if 'ai_enabled' in body:
            record['ai_enabled'] = bool(body.get('ai_enabled'))
        existing.append(record)
        cfg['holdings'] = existing
        _save_agent_cfg(cfg)
        return jsonify({'ok': True})

    if request.method == 'DELETE':
        code = request.args.get('code', '').strip().upper()
        cfg['holdings'] = [h for h in cfg.get('holdings', []) if h.get('code') != code]
        _monitor_drop_code(code)   # 刪除持倉 → 同步移出盤中監控
        _save_agent_cfg(cfg)
        return jsonify({'ok': True})


@app.route('/api/agent/holdings/ai_toggle', methods=['POST'])
def agent_holding_ai_toggle_api():
    """單檔持倉切換 AI 分析監測開關（不重抓報價，省 token＋秒回）。
    ai_enabled=False 的持倉在晨報/盤中/盤後統合中完全略過，不呼叫 Opus。"""
    body = request.get_json(force=True)
    code = str(body.get('code', '')).strip().upper().replace('.TW', '').replace('.TWO', '')
    enabled = bool(body.get('enabled', True))
    if not code:
        return jsonify({'error': '代碼不可為空'}), 400
    cfg = _load_agent_cfg()
    found = False
    for h in cfg.get('holdings', []):
        if str(h.get('code', '')).strip().upper() == code:
            h['ai_enabled'] = enabled
            found = True
    if not found:
        return jsonify({'error': '查無此持倉'}), 404
    _save_agent_cfg(cfg)
    return jsonify({'ok': True, 'code': code, 'ai_enabled': enabled})


def _monitor_drop_code(code: str):
    """持倉全數賣出／刪除時，把該檔一併移出盤中監控清單，避免賣掉後還一直被推
    「分批了結／賣出」訊號（持倉與監控是兩份清單，過去賣出只清持倉沒清監控）。"""
    base = (code or '').upper().replace('.TW', '').replace('.TWO', '')
    if not base:
        return
    with _monitor_lock:
        mcfg = _load_monitor_cfg()
        tickers = mcfg.get('tickers', {})
        drop = [k for k in tickers
                if k.upper().replace('.TW', '').replace('.TWO', '') == base]
        if drop:
            for k in drop:
                tickers.pop(k, None)
            _save_monitor_cfg(mcfg)


@app.route('/api/agent/sell', methods=['POST'])
def agent_sell_api():
    """賣出／獲利了結：扣減持倉股數，並把這筆實現損益寫進 sells 帳本。

    刪除持倉只會讓系統「忘記」你曾持有，無法得知賣多少、賺多少；改走這支才會留下
    可累計的已實現損益紀錄。支援分批賣（賣一部分股數，剩餘成本均價不變）。"""
    cfg  = _load_agent_cfg()
    body = request.get_json(force=True) or {}
    code = body.get('code', '').strip().upper().replace('.TW', '').replace('.TWO', '')
    if not code:
        return jsonify({'error': '代碼不可為空'}), 400

    h = next((x for x in cfg.get('holdings', []) if x.get('code') == code), None)
    if not h:
        return jsonify({'error': f'找不到持倉 {code}'}), 404

    held       = safe_float(h.get('shares', 0))
    sell_price = safe_float(body.get('sell_price', 0))
    # 賣出股數未填或 <=0 視為「全賣」
    sell_shares = safe_float(body.get('shares', 0))
    if sell_shares <= 0:
        sell_shares = held
    if sell_price <= 0:
        return jsonify({'error': '請輸入有效賣出價格'}), 400
    if sell_shares > held + 1e-6:
        return jsonify({'error': f'賣出股數 {sell_shares:g} 超過持有 {held:g}'}), 400

    buy_price     = safe_float(h.get('buy_price', 0))
    proceeds      = sell_price * sell_shares
    cost_basis    = buy_price * sell_shares
    realized_pnl  = round(proceeds - cost_basis, 2)
    realized_pct  = round((sell_price - buy_price) / buy_price * 100, 2) if buy_price else 0
    sell_date     = body.get('date') or pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d')

    record = {
        'id':           pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y%m%d%H%M%S%f'),
        'date':         sell_date,
        'code':         code,
        'name':         h.get('name', code),
        'shares':       sell_shares,
        'sell_price':   round(sell_price, 4),
        'buy_price':    round(buy_price, 4),
        'proceeds':     round(proceeds, 2),
        'cost_basis':   round(cost_basis, 2),
        'realized_pnl': realized_pnl,
        'realized_pct': realized_pct,
        'note':         body.get('note', ''),
    }
    cfg.setdefault('sells', []).append(record)

    # 扣減持倉；全賣則移除該檔
    remaining = round(held - sell_shares, 6)
    if remaining <= 1e-6:
        cfg['holdings'] = [x for x in cfg.get('holdings', []) if x.get('code') != code]
        _monitor_drop_code(code)   # 全數賣出 → 同步移出盤中監控，不再推賣出訊號
    else:
        h['shares'] = int(remaining) if float(remaining).is_integer() else remaining
    _save_agent_cfg(cfg)
    return jsonify({'ok': True, 'realized_pnl': realized_pnl,
                    'realized_pct': realized_pct, 'remaining': max(remaining, 0)})


@app.route('/api/agent/sells', methods=['GET', 'DELETE'])
def agent_sells_api():
    """已實現損益帳：GET 回傳全部賣出紀錄＋彙總；DELETE?id= 刪除誤記的某一筆。"""
    cfg = _load_agent_cfg()
    if request.method == 'DELETE':
        sid = request.args.get('id', '')
        cfg['sells'] = [s for s in cfg.get('sells', []) if s.get('id') != sid]
        _save_agent_cfg(cfg)
        return jsonify({'ok': True})

    sells = sorted(cfg.get('sells', []), key=lambda s: s.get('date', ''), reverse=True)
    total_pnl  = round(sum(safe_float(s.get('realized_pnl', 0)) for s in sells), 2)
    total_cost = sum(safe_float(s.get('cost_basis', 0)) for s in sells)
    total_pct  = round(total_pnl / total_cost * 100, 2) if total_cost else 0
    return jsonify({'sells': sells, 'total_pnl': total_pnl,
                    'total_pct': total_pct, 'count': len(sells)})


@app.route('/api/agent/scan', methods=['POST'])
def agent_scan_api():
    cfg = _load_agent_cfg()
    body = request.get_json(force=True) or {}
    for field in ['scan_price_min', 'scan_price_max', 'scan_min_score', 'scan_universe']:
        if field in body:
            cfg[field] = body[field]
    results = _run_agent_scan(cfg)
    cfg['candidates'] = results
    cfg['last_scan'] = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M')
    _save_agent_cfg(cfg)
    return jsonify(results)


@app.route('/api/agent/analyze/<code>')
def agent_analyze_api(code):
    code = code.strip().upper().replace('.TW', '').replace('.TWO', '')
    cfg  = _load_agent_cfg()
    api_key = request.args.get('key', '') or cfg.get('claude_api_key', '')
    if not api_key:
        return jsonify({'error': '未設定 Claude API Key'}), 400

    import anthropic
    cfg_h    = _load_agent_cfg()
    holding  = next((h for h in cfg_h.get('holdings', []) if h.get('code') == code), None)
    data     = _fetch_predict_data(code)
    ctx      = _agent_recommendation_text(data, holding)

    prompt   = f'請針對以下股票給出明日操作建議：\n\n{ctx}'
    if holding:
        prompt += '\n\n這是我目前持有的股票，請特別說明是否應該持有、加碼或減碼。'
    else:
        prompt += '\n\n這不是我的持倉，請評估是否值得買進。'

    def generate():
        try:
            client = anthropic.Anthropic(api_key=api_key)
            with client.messages.stream(
                model='claude-opus-4-8',
                max_tokens=2000,
                system=_agent_system_prompt(data),
                messages=[{'role': 'user', 'content': prompt}],
            ) as stream:
                for text in stream.text_stream:
                    yield f"data: {json.dumps({'text': _strip_emoji(text)})}\n\n"
            yield 'data: [DONE]\n\n'
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    from flask import Response, stream_with_context
    return Response(stream_with_context(generate()), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@app.route('/api/agent/chat', methods=['POST'])
def agent_chat_api():
    import anthropic
    from flask import Response, stream_with_context
    cfg     = _load_agent_cfg()
    body    = request.get_json(force=True)
    api_key = body.get('api_key', '') or _agent_api_key(cfg)
    msgs    = body.get('messages', [])

    if not api_key:
        return jsonify({'error': '未設定 Claude API Key'}), 400

    # 把目前持倉＋已實現買賣帳本帶給 AI，讓對話看得到部位、能討論買賣（不再「偵測不到持股」）
    system = _AGENT_SYSTEM_PROMPT + '\n\n' + _goal_chat_context(cfg)

    def generate():
        try:
            client = anthropic.Anthropic(api_key=api_key)
            with client.messages.stream(
                model='claude-opus-4-8',
                max_tokens=1000,
                system=system,
                messages=msgs,
            ) as stream:
                for text in stream.text_stream:
                    yield f"data: {json.dumps({'text': _strip_emoji(text)})}\n\n"
            yield 'data: [DONE]\n\n'
        except anthropic.AuthenticationError:
            yield f"data: {json.dumps({'error': 'API Key 無效'})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@app.route('/api/agent/notifications')
def agent_notifications_api():
    cfg = _load_agent_cfg()
    return jsonify(cfg.get('notifications', []))


@app.route('/api/agent/daily_strategy')
def agent_daily_strategy_api():
    cfg = _load_agent_cfg()
    return jsonify(cfg.get('daily_strategy', {}))


@app.route('/api/agent/goal', methods=['GET', 'POST'])
def agent_goal_api():
    """讀取/設定目標（金額、期限、現金、風險偏好）。"""
    cfg = _load_agent_cfg()
    if request.method == 'GET':
        return jsonify(cfg.get('goal', {}))
    body = request.get_json(force=True) or {}
    goal = cfg.get('goal', {}) or {}
    for k in ('enabled', 'start_capital', 'cash', 'target_amount',
              'start_date', 'target_date', 'risk'):
        if k in body:
            goal[k] = body[k]
    cfg['goal'] = goal
    _save_agent_cfg(cfg)
    return jsonify({'ok': True, 'goal': goal})


@app.route('/api/agent/goal/status')
def agent_goal_status_api():
    """回傳目標 + 持倉快照 + 達標試算（即時計算，含明日買賣計畫快取）。"""
    cfg = _load_agent_cfg()
    status = _goal_status(cfg)
    g = cfg.get('goal', {})
    status['last_plan']   = g.get('last_plan', '')
    status['last_review'] = g.get('last_review', '')
    rs = g.get('review_status', '')
    # 保險：若檢討卡在 running 超過 20 分鐘（多半是行程重啟導致背景執行緒中斷），
    # 視為逾時，避免前端永遠輪詢
    if rs == 'running':
        started = g.get('review_started', '')
        try:
            age = (pd.Timestamp.now(tz='Asia/Taipei') - pd.Timestamp(started, tz='Asia/Taipei')).total_seconds()
            if started and age > 1200:
                rs = 'error'
        except Exception:
            pass
    status['review_status'] = rs
    return jsonify(status)


@app.route('/api/agent/goal/chat', methods=['POST'])
def agent_goal_chat_api():
    """目標頁 AI 討論：自動把目標進度＋持倉＋已實現買賣帳本帶給 AI，讓它看得到部位、能檢討買賣。"""
    import anthropic
    from flask import Response, stream_with_context
    cfg     = _load_agent_cfg()
    body    = request.get_json(force=True) or {}
    api_key = body.get('api_key', '') or _agent_api_key(cfg)
    msgs    = body.get('messages', [])

    if not api_key:
        return jsonify({'error': '未設定 Claude API Key'}), 400

    system = _GOAL_CHAT_SYSTEM_PROMPT + '\n\n' + _goal_chat_context(cfg)

    def generate():
        try:
            client = anthropic.Anthropic(api_key=api_key)
            with client.messages.stream(
                model='claude-opus-4-8',
                max_tokens=1200,
                system=system,
                messages=msgs,
            ) as stream:
                for text in stream.text_stream:
                    yield f"data: {json.dumps({'text': _strip_emoji(text)})}\n\n"
            yield 'data: [DONE]\n\n'
        except anthropic.AuthenticationError:
            yield f"data: {json.dumps({'error': 'API Key 無效'})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@app.route('/api/market/overnight')
def market_overnight_api():
    """昨夜美股快照與對台股偏向（供晨報、目標檢討、前端 banner 使用）。"""
    return jsonify(_us_overnight_snapshot())


@app.route('/api/agent/goal/review_now', methods=['POST'])
def agent_goal_review_now():
    """手動觸發目標導向檢討（背景執行，完成後推 LINE 並更新快取）。"""
    cfg = _load_agent_cfg()
    api_key = _agent_api_key(cfg)
    if not api_key:
        return jsonify({'error': '未設定 Claude API Key'}), 400
    if not cfg.get('goal', {}).get('enabled'):
        return jsonify({'error': '尚未啟用目標導向操盤，請先設定並啟用目標'}), 400

    # 立刻標記為「進行中」，前端才知道正在跑（而非「尚無報告」）
    cfg.setdefault('goal', {})
    if cfg['goal'].get('review_status') == 'running':
        return jsonify({'ok': True, 'message': '已有一份檢討正在進行中，請稍候'})
    cfg['goal']['review_status']  = 'running'
    cfg['goal']['review_started'] = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M:%S')
    _save_agent_cfg(cfg)

    def _bg():
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            _run_goal_review(_load_agent_cfg(), client)
        except Exception as e:
            print(f'[Agent] goal review_now error: {e}')
            c = _load_agent_cfg()
            c.setdefault('goal', {})
            c['goal']['review_status'] = 'error'
            c['goal']['last_plan'] = f'檢討失敗：{e}'
            _save_agent_cfg(c)

    threading.Thread(target=_bg, daemon=True).start()
    return jsonify({'ok': True, 'message': '目標檢討已在背景執行，完成後會自動顯示並推播明日買賣計畫'})


@app.route('/api/agent/optimize_now', methods=['POST'])
def agent_optimize_now():
    """手動觸發每日策略優化（背景執行）。"""
    cfg = _load_agent_cfg()
    api_key = cfg.get('claude_api_key', '')
    if not api_key:
        return jsonify({'error': '未設定 Claude API Key'}), 400

    def _bg():
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            _run_daily_optimizer(_load_agent_cfg(), client)
        except Exception as e:
            print(f'[Agent] optimize_now error: {e}')

    threading.Thread(target=_bg, daemon=True).start()
    return jsonify({'ok': True, 'message': '策略優化已在背景執行（約 1-3 分鐘），完成後會更新並推播'})


@app.route('/api/agent/run_now', methods=['POST'])
def agent_run_now():
    """Manually trigger a full analysis cycle."""
    cfg = _load_agent_cfg()
    api_key = cfg.get('claude_api_key', '')
    if not api_key:
        return jsonify({'error': '未設定 Claude API Key'}), 400

    def _bg():
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            c = _load_agent_cfg()
            analysis  = _run_agent_holdings_analysis(c, client)
            candidates = _run_agent_scan(c)
            scan_ai   = _run_agent_scan_with_ai(c, client)

            today = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M')
            lines = [f'[{today}] 手動觸發分析', '']

            if analysis:
                lines.append('--- 持倉建議 ---')
                for h in analysis:
                    lines.append(f"{h['name']} 現價:{h['price']} 損益:{h['pnl_pct']:+.1f}%")
                    lines.append(h['recommendation'][:250])
                    lines.append('')

            lines.append('--- 掃描結果 ---')
            lines.append(scan_ai[:500])

            c['candidates'] = candidates
            c['last_scan']  = today
            _agent_notify(c, '手動分析完成', '\n'.join(lines))
        except Exception as e:
            print(f'[Agent] run_now error: {e}')

    threading.Thread(target=_bg, daemon=True).start()
    return jsonify({'ok': True, 'message': '分析已在背景執行，完成後推播 LINE 通知'})


# ═══════════════════════════════════════════════════════════════════════════
#  統一 AI 股票系統 — Tool Use 編排層
# ═══════════════════════════════════════════════════════════════════════════

# 精選條件目錄（從 110+ 條件中挑出最實用的，供 Claude 自動挑選組合）
CONDITION_CATALOG = """可用的篩選條件（type 為條件代碼，params 為可選參數，省略則用預設值）：

【趨勢與均線】
- price_above_ma {period:20} 股價站上 N 日均線
- price_cross_above_ma {period:20} 股價剛突破均線（黃金交叉價）
- ma_trending_up {period:20, trend_days:5} 均線向上彎
- ma_bull_alignment 均線多頭排列（5>10>20>60）
- ma_golden_cross {short_period:5, long_period:20} 短均線黃金交叉長均線
- monthly_price_above_ma 站上月線（月K）
- ma_deduction_up {period:20} 月線扣抵向上（均線明日將翻揚，趨勢轉強的先行訊號）
- pullback_hold_ma {period:20, within_pct:3} 回測均線守穩（現價貼近均線上方，回後續攻）

【動能指標】
- kd_golden_cross KD 黃金交叉
- kd_low_golden_cross {threshold:30} KD 低檔黃金交叉（K<門檻才算，起漲訊號更精準）
- kd_k_below {threshold:20} KD 的 K 值低檔（超賣）
- macd_golden_cross MACD 黃金交叉
- macd_bullish MACD 多頭（DIF>MACD）
- rsi_below {threshold:30} RSI 超賣
- rsi_cross_above {threshold:50} RSI 向上突破
- william_r_oversold 威廉指標超賣
- cci_oversold CCI 超賣
- bias_low {threshold:-5} 乖離率偏低（跌深）
- mtm_cross_above 動量 MTM 由負翻正
- weekly_kd_golden_cross 週 KD 黃金交叉（中期）
- monthly_kd_oversold 月 KD 低檔（長期低基期）

【量價結構】
- volume_ratio_above {ratio:1.5} 爆量（量 > N 倍均量）
- vol_5_above_20 量能結構轉強（5日均量站上20日均量）
- volume_shrinking 量縮（賣壓宣洩）
- high_vol_breakout 帶量突破前高
- vol_up_candle 量增收紅
- obv_rising OBV 能量潮上升
- price_consolidation_break 突破盤整區
- bb_squeeze 布林通道收口（變盤前）
- bb_breakout_up 突破布林上軌
- price_near_bb_lower 接近布林下軌（低接）
- bb_oversold 布林超賣

【籌碼面】
- inst_foreign_buy 外資今日買超
- inst_trust_buy 投信今日買超
- inst_3_buy 三大法人今日同步買超
- inst_total_above {threshold:1000} 法人合計買超超過 N 千股
- inst_foreign_dominant 外資主導買超
- inst_foreign_buy_ndays {days:3} 外資「連續N日」買超（連買，比單日有力）
- inst_trust_buy_ndays {days:3} 投信「連續N日」買超
- inst_3_buy_ndays {days:3} 三大法人「連續N日」買超
- inst_net_sum_above {days:5, threshold:5000} 近N日法人累計買超超過 X 千股
- margin_decrease 融資減少（散戶退場）
- short_decrease 融券/借券減少（空方回補）
- inst_pct_above {threshold:20} 法人持股比例高

【K 線型態】
- candle_hammer 鎚子線（底部反轉）
- candle_bullish_engulfing 多頭吞噬
- candle_morning_star 晨星（底部）
- candle_long_lower_wick 長下影線（低檔買盤）
- consecutive_up {days:3} 連續上漲 N 天

【位階與價值】
- price_near_52w_low 接近 52 週低點
- price_from_high_below {threshold:20} 距高點回檔超過 N%
- price_range {min:10, max:100} 股價在指定區間
- pe_below {threshold:15} 本益比偏低
- div_yield_above {threshold:4} 殖利率高於 N%

挑選原則：起漲股常用組合 = ma_trending_up + kd_golden_cross + volume_ratio_above + inst_3_buy；
低接股 = rsi_below + price_near_bb_lower + candle_hammer；
強勢突破 = high_vol_breakout + ma_bull_alignment + macd_golden_cross。
一次挑 2-4 個條件即可，太多會篩不出標的。"""


# 產業分類清單（給 AI 參考可用的 sector 名稱）
def _sector_list_text() -> str:
    return '、'.join(TW_SCREENER_UNIVERSE.keys())


_AI_SYSTEM_PROMPT = f"""你是一位專業的台股 AI 投資總管，能主動運用工具幫使用者選股、分析、追蹤持倉。

你的核心能力：使用者用白話描述需求（例如「幫我找低價剛起漲的半導體股」），
你要自行判斷該用哪些篩選條件，呼叫 screen_stocks 工具執行，再用 analyze_stock 深入分析最佳標的，
用 backtest_strategy 以歷史數據驗證最賺錢的操作方式，必要時用 get_stock_news 查消息面，最後給出明確建議。

若使用者上傳 K 線圖或籌碼截圖：先判讀圖中的均線排列、K 線型態（長上下影線、十字星、紅黑實體）、
量價關係與任何可見的指標數值，並用以下框架分析：月線扣抵環境、五大起漲指標、量價動能、法人籌碼。
若圖中能辨識出股票代碼或名稱，可再呼叫 analyze_stock / backtest_strategy 取得即時數據交叉驗證。

{CONDITION_CATALOG}

可用產業分類（sector 參數）：{_sector_list_text()}

工作流程建議：
1. 理解使用者意圖 → 決定 scope（sector 指定產業 / market 全市場）與條件組合
2. 呼叫 screen_stocks 篩選 → 取得候選清單與 15 因子評分等級（A/B/C/D/F）
3. 對前 2-3 名呼叫 analyze_stock 取得完整數據 + 必要時 get_stock_news 查新聞
4. 對最佳標的呼叫 backtest_strategy → 用歷史回測找出該檔最適合的進出場方式
   （例如某檔用「停損10%停利20%」報酬最高，另一檔可能「移動停利」或「固定持有」更好）
5. 綜合給建議：明確標的、進場理由、關鍵支撐壓力價、預估明日區間、
   並依回測結果建議「具體的停損停利或持有策略」、風險提示

回應原則：
- 使用繁體中文，語氣專業果斷，避免模糊
- 全程不要使用任何 emoji 或表情符號（包括星號、警示、圖釘等各類符號），改用文字標示重點
- 推薦個股時務必說明「為什麼是這檔」與「關鍵價位」
- 若使用者問持倉，先呼叫 get_holdings 取得實際部位再分析
- 所有建議僅供參考，最後可附簡短風險提醒"""


# ── Tool schemas（給 Claude 的工具定義）──────────────────────────────────
_AI_TOOLS = [
    {
        'name': 'screen_stocks',
        'description': '依指定的技術/籌碼條件篩選台股。可選擇掃描特定產業或全市場成交量前段班。回傳通過條件的股票及其 15 因子評分等級。',
        'input_schema': {
            'type': 'object',
            'properties': {
                'conditions': {
                    'type': 'array',
                    'description': '篩選條件清單，每項為 {type, params}。type 必填，params 可選。',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'type':   {'type': 'string', 'description': '條件代碼，見系統提示的條件目錄'},
                            'params': {'type': 'object', 'description': '參數（可省略用預設）'},
                        },
                        'required': ['type'],
                    },
                },
                'scope':     {'type': 'string', 'enum': ['sector', 'market'], 'description': 'sector=指定產業, market=全市場'},
                'sector':    {'type': 'string', 'description': 'scope=sector 時的產業名稱'},
                'price_min': {'type': 'number', 'description': '最低股價篩選，預設 0'},
                'price_max': {'type': 'number', 'description': '最高股價篩選，預設 9999'},
            },
            'required': ['conditions', 'scope'],
        },
    },
    {
        'name': 'analyze_stock',
        'description': '取得單一台股的完整技術面、籌碼面數據與 15 因子評分明細，用於深入分析個股。',
        'input_schema': {
            'type': 'object',
            'properties': {
                'code': {'type': 'string', 'description': '台股代碼，如 2330'},
            },
            'required': ['code'],
        },
    },
    {
        'name': 'get_stock_news',
        'description': '查詢個股或關鍵字的近期新聞標題，用於評估消息面影響。',
        'input_schema': {
            'type': 'object',
            'properties': {
                'query': {'type': 'string', 'description': '股票名稱或關鍵字，如 台積電、AI 伺服器'},
            },
            'required': ['query'],
        },
    },
    {
        'name': 'get_holdings',
        'description': '取得使用者目前的持倉部位、買入價與即時損益。當使用者詢問持倉相關問題時使用。',
        'input_schema': {'type': 'object', 'properties': {}},
    },
    {
        'name': 'backtest_strategy',
        'description': '對個股用歷史資料回測，比較多種出場策略（百分比停損停利、停損不停利、固定持有期限、移動停利）哪一種報酬最高。在給進出場建議前呼叫，可用實證數據決定該檔最適合的操作方式。',
        'input_schema': {
            'type': 'object',
            'properties': {
                'code':   {'type': 'string', 'description': '台股代碼，如 2330'},
                'signal': {'type': 'string',
                           'enum': ['kd_gc', 'ma_gc', 'macd_gc', 'rsi_recover', 'breakout_ma20', 'buy_hold'],
                           'description': '進場訊號：kd_gc=KD黃金交叉, ma_gc=MA5上穿MA20, macd_gc=MACD黃金交叉, rsi_recover=RSI由超賣回升, breakout_ma20=突破月線, buy_hold=買進持有(純比較出場)'},
                'period': {'type': 'string', 'description': '回測期間，如 3y、5y，預設 3y'},
            },
            'required': ['code'],
        },
    },
]


def _tool_screen_stocks(conditions, scope='sector', sector='', price_min=0, price_max=9999):
    """執行篩選工具，回傳通過條件並含 15 因子評分的股票清單。"""
    price_min = safe_float(price_min, 0)
    price_max = safe_float(price_max, 9999) or 9999

    # 決定掃描範圍
    if scope == 'market':
        codes = _get_tw_universe('top100')
    else:
        if sector and sector in TW_SCREENER_UNIVERSE:
            codes = TW_SCREENER_UNIVERSE[sector]
        else:
            # 找不到指定產業，退回全市場
            codes = _get_tw_universe('top100')

    if not codes:
        return {'error': '無法取得掃描清單', 'matches': []}

    tickers = [tw_normalize(c) for c in codes]
    tickers = list(dict.fromkeys(tickers))[:120]

    # 正規化條件格式
    norm_conds = []
    for c in (conditions or []):
        if isinstance(c, dict) and c.get('type'):
            norm_conds.append({'type': c['type'], 'params': c.get('params', {}) or {}})
    if not norm_conds:
        return {'error': '未提供有效條件', 'matches': []}

    # 第一階段：條件篩選
    matched = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(_scan_ticker, t, norm_conds, True): t for t in tickers}
        for f in as_completed(futs):
            try:
                r = f.result()
                if r and price_min <= r.get('price', 0) <= price_max:
                    matched.append(r)
            except Exception:
                pass

    if not matched:
        return {'matched': 0, 'matches': [],
                'note': '無股票同時符合所有條件，可嘗試減少條件數量或放寬參數'}

    # 取漲幅前段，第二階段補 15 因子評分
    matched.sort(key=lambda x: x.get('changePct', 0), reverse=True)
    top = matched[:12]

    results = []
    def _enrich(r):
        code = r['ticker'].replace('.TW', '').replace('.TWO', '')
        data = _fetch_predict_data(code)
        mf   = data.get('mf_score') or {}
        return {
            'code':      code,
            'name':      r.get('name', code),
            'price':     r.get('price'),
            'changePct': r.get('changePct'),
            'grade':     mf.get('grade', '?'),
            'score':     mf.get('total', 0),
            'max_score': mf.get('max', 24),
            'bias20':    data.get('bias20'),
            'rebound':   data.get('rebound_pct'),
            'kd':        f"K{data.get('k','?')} D{data.get('d','?')}",
            'rsi':       data.get('rsi'),
            'ma20_trend': data.get('ma20_trend', ''),
            'inst_total': (data.get('inst') or {}).get('total_net'),
            'passed':    [f['name'] for f in mf.get('breakdown', []) if f['pass']],
        }
    with ThreadPoolExecutor(max_workers=6) as ex:
        for r in ex.map(_enrich, top):
            results.append(r)

    grade_order = {'A': 4, 'B': 3, 'C': 2, 'D': 1, 'F': 0, '?': -1}
    results.sort(key=lambda x: -grade_order.get(x['grade'], -1))
    return {'matched': len(matched), 'matches': results}


def _tool_analyze_stock(code):
    """單檔深入分析，回傳精簡的關鍵數據與 15 因子明細。"""
    code = str(code).strip().upper().replace('.TW', '').replace('.TWO', '')
    data = _fetch_predict_data(code)
    if data.get('error'):
        return {'error': data['error']}
    mf = data.get('mf_score') or {}
    return {
        'code':   code,
        'name':   data.get('name', code),
        'price':  data.get('price'),
        'grade':  mf.get('grade', '?'),
        'score':  f"{mf.get('total',0)}/{mf.get('max',24)}",
        'ma20_trend':   data.get('ma20_trend'),
        'ma20_deduct':  data.get('ma20_deduct_price'),
        'bias20':       data.get('bias20'),
        'rebound_pct':  data.get('rebound_pct'),
        'kd':           f"K{data.get('k')} D{data.get('d')} {data.get('kd_status','')}",
        'week_kd':      f"週K{data.get('week_k')} 週D{data.get('week_d')}",
        'rsi':          data.get('rsi'),
        'macd_osc':     f"{data.get('osc')} {data.get('osc_trend','')}",
        'bb_pct':       data.get('bb_pct'),
        'w52_pct':      data.get('w52_pct'),
        'ma_bull':      data.get('ma_bull'),
        'vol_structure': data.get('vol_structure'),
        'vol_ratio':    data.get('vol_ratio'),
        'day_trade_ratio': data.get('day_trade_ratio'),
        'support_5d':   data.get('low_5d'),
        'resist_5d':    data.get('high_5d'),
        'support_20d':  data.get('low_20d'),
        'resist_20d':   data.get('high_20d'),
        'ma':           f"MA5={data.get('ma5')} MA20={data.get('ma20')} MA60={data.get('ma60')}",
        'inst':         data.get('inst'),
        'margin':       data.get('margin'),
        'factors_pass': [f['name'] for f in mf.get('breakdown', []) if f['pass']],
        'factors_fail': [f['name'] for f in mf.get('breakdown', []) if not f['pass']],
    }


def _tool_get_news(query):
    """查新聞工具。"""
    news = _fetch_gnews(str(query), max_results=8)
    return {'query': query,
            'news': [{'title': a['title'], 'publisher': a.get('publisher', ''),
                      'time': a.get('pubTime', '')} for a in news]}


def _tool_get_holdings():
    """取得持倉與即時損益。"""
    cfg = _load_agent_cfg()
    holdings = cfg.get('holdings', [])
    out = []
    for h in holdings:
        code = h.get('code', '')
        try:
            data = _fetch_predict_data(code)
            cur  = data.get('price', 0)
            name = data.get('name', code)
        except Exception:
            cur, name = 0, code
        buy_p = h.get('buy_price', 0)
        pnl   = round((cur - buy_p) / buy_p * 100, 2) if buy_p and cur else 0
        out.append({'code': code, 'name': name, 'buy_price': buy_p,
                    'shares': h.get('shares', 0), 'current_price': cur, 'pnl_pct': pnl})
    return {'holdings': out, 'count': len(out)}


# ═══════════════════════════════════════════════════════════════════════════
#  策略回測引擎
# ═══════════════════════════════════════════════════════════════════════════

# ── 進場訊號（向量化，可回測。籌碼面因無歷史逐日資料故不納入）────────────────
def _cross_up(a, b):
    """a 由下往上穿越 b（b 可為 Series 或純量）。回傳 boolean Series。"""
    if np.isscalar(b):
        return (a.shift(1) <= b) & (a > b)
    return (a.shift(1) <= b.shift(1)) & (a > b)


def _bt_indicators(hist):
    """一次算好回測訊號需要的所有技術指標序列。"""
    close = hist['Close']; high = hist['High']; low = hist['Low']
    openp = hist['Open'];  vol  = hist['Volume']
    k, d = calc_kd(high, low, close)
    macd, macd_sig, _ = calc_macd(close)
    bb_up, _bb_mid, bb_low = calc_bollinger(close)
    return {
        'open': openp, 'close': close, 'high': high, 'low': low, 'vol': vol,
        'ma5':  close.rolling(5,  min_periods=1).mean(),
        'ma10': close.rolling(10, min_periods=1).mean(),
        'ma20': close.rolling(20, min_periods=1).mean(),
        'ma60': close.rolling(60, min_periods=1).mean(),
        'vma5':  vol.rolling(5,  min_periods=1).mean(),
        'vma20': vol.rolling(20, min_periods=1).mean(),
        'k': k, 'd': d, 'macd': macd, 'macd_sig': macd_sig,
        'rsi': calc_rsi(close), 'bb_up': bb_up, 'bb_low': bb_low,
    }


# 每個訊號 fn(ctx) -> boolean Series（該根 K 棒是否觸發進場）
def _sig_ma_gc(c):         return _cross_up(c['ma5'], c['ma20'])
def _sig_ma_bull(c):
    a = (c['ma5'] > c['ma10']) & (c['ma10'] > c['ma20']) & (c['ma20'] > c['ma60'])
    return a & ~a.shift(1).fillna(False)
def _sig_ma20_turn_up(c):
    s = c['ma20'].diff()
    return (s > 0) & (s.shift(1) <= 0)
def _sig_pullback_ma20(c):
    gap  = (c['close'] - c['ma20']) / c['ma20']
    cond = (gap >= 0) & (gap <= 0.03) & (c['ma20'].diff() > 0)
    return cond & ~cond.shift(1).fillna(False)
def _sig_breakout_ma20(c): return _cross_up(c['close'], c['ma20'])
def _sig_breakout_ma60(c): return _cross_up(c['close'], c['ma60'])

def _sig_kd_gc(c):         return _cross_up(c['k'], c['d'])
def _sig_kd_low_gc(c):     return _cross_up(c['k'], c['d']) & (c['k'] < 35)
def _sig_kd_os_up(c):      return _cross_up(c['k'], 20)
def _sig_macd_gc(c):       return _cross_up(c['macd'], c['macd_sig'])
def _sig_macd_zero(c):     return _cross_up(c['macd'], 0)
def _sig_rsi_recover(c):   return (c['rsi'].shift(1) <= 30) & (c['rsi'] > 30)
def _sig_rsi_cross50(c):   return _cross_up(c['rsi'], 50)
def _sig_mtm_up(c):        return _cross_up(c['close'] - c['close'].shift(10), 0)

def _sig_vol_spike(c):     return (c['vol'] > 1.5 * c['vma20']) & (c['close'] > c['close'].shift(1))
def _sig_high_vol_breakout(c):
    prior_high = c['high'].rolling(20).max().shift(1)
    return (c['close'] > prior_high) & (c['vol'] > 1.5 * c['vma20'])
def _sig_vol5_cross20(c):  return _cross_up(c['vma5'], c['vma20'])
def _sig_bb_break_up(c):   return _cross_up(c['close'], c['bb_up'])
def _sig_bb_os_recover(c): return (c['close'].shift(1) <= c['bb_low'].shift(1)) & (c['close'] > c['bb_low'])

def _sig_hammer(c):
    omin = np.minimum(c['open'], c['close'])
    omax = np.maximum(c['open'], c['close'])
    body  = (c['close'] - c['open']).abs()
    lower = omin - c['low']
    upper = c['high'] - omax
    rng   = c['high'] - c['low']
    return (lower >= 2 * body) & (upper <= body) & (rng > 0) & (body > 0)
def _sig_engulf(c):
    po = c['open'].shift(1); pc = c['close'].shift(1)
    return (pc < po) & (c['close'] > c['open']) & (c['close'] >= po) & (c['open'] <= pc)
def _sig_up3(c):
    up = c['close'] > c['close'].shift(1)
    return up & up.shift(1) & up.shift(2)


# 分組目錄（前端勾選用）。code 同時是回測訊號代碼。
_SIGNAL_DEFS = [
    ('趨勢與均線', [
        ('ma_gc',         'MA5 上穿 MA20（黃金交叉）',        _sig_ma_gc),
        ('ma_bull',       '均線多頭排列成形（5>10>20>60）',   _sig_ma_bull),
        ('ma20_turn_up',  '月線(MA20)翻揚向上',               _sig_ma20_turn_up),
        ('pullback_ma20', '回測月線守穩（貼月線上方續攻）',   _sig_pullback_ma20),
        ('breakout_ma20', '股價當日突破月線(MA20)（剛上穿/事件，非「站上」）', _sig_breakout_ma20),
        ('breakout_ma60', '股價當日突破季線(MA60)（剛上穿/事件，要「站上季線」請用下方均線狀態條件）', _sig_breakout_ma60),
    ]),
    ('動能指標', [
        ('kd_gc',       'KD 黃金交叉',                        _sig_kd_gc),
        ('kd_low_gc',   'KD 低檔黃金交叉（K<35，起漲更準）',  _sig_kd_low_gc),
        ('kd_os_up',    'KD 的 K 值由超賣(20)回升',           _sig_kd_os_up),
        ('macd_gc',     'MACD 黃金交叉',                      _sig_macd_gc),
        ('macd_zero',   'MACD 由負翻正（穿越 0 軸）',         _sig_macd_zero),
        ('rsi_recover', 'RSI 由 30 以下回升',                 _sig_rsi_recover),
        ('rsi_cross50', 'RSI 向上突破 50（轉強）',            _sig_rsi_cross50),
        ('mtm_up',      '動量 MTM 由負翻正',                  _sig_mtm_up),
    ]),
    ('量價結構', [
        ('vol_spike',         '爆量收紅（量 > 1.5 倍均量）',      _sig_vol_spike),
        ('high_vol_breakout', '帶量突破近 20 日新高',            _sig_high_vol_breakout),
        ('vol5_cross20',      '量能轉強（5 日均量站上 20 日）',   _sig_vol5_cross20),
        ('bb_break_up',       '突破布林上軌',                    _sig_bb_break_up),
        ('bb_os_recover',     '布林下軌低接反彈',                _sig_bb_os_recover),
    ]),
    ('K 線型態', [
        ('hammer', '鎚子線（底部反轉）', _sig_hammer),
        ('engulf', '多頭吞噬',          _sig_engulf),
        ('up3',    '連續上漲 3 天',      _sig_up3),
    ]),
]

SIGNAL_FNS    = {code: fn    for _g, items in _SIGNAL_DEFS for code, _l, fn  in items}
ENTRY_SIGNALS = {code: label for _g, items in _SIGNAL_DEFS for code, label, _fn in items}
ENTRY_SIGNALS['buy_hold'] = '買進並持有（基準）'
SIGNAL_GROUPS_PUBLIC = [
    {'name': g, 'signals': [{'code': code, 'label': label} for code, label, _fn in items]}
    for g, items in _SIGNAL_DEFS
]


def _compute_entry_mask(hist, signal):
    """signal 可為單一代碼或代碼清單；清單時逐根 AND（全部滿足當天才進場）。
    回傳 boolean numpy 陣列。"""
    codes = list(signal) if isinstance(signal, (list, tuple)) else [signal]
    codes = [c for c in codes if c]
    n = len(hist)
    if codes == ['buy_hold']:
        mask = np.zeros(n, dtype=bool)
        if n: mask[0] = True
        return mask
    codes = [c for c in codes if c != 'buy_hold'] or ['kd_gc']
    ctx = _bt_indicators(hist)
    out = None
    for code in codes:
        fn = SIGNAL_FNS.get(code)
        if fn is None:
            continue
        m = fn(ctx).fillna(False).to_numpy(dtype=bool)
        out = m if out is None else (out & m)
    return out if out is not None else np.zeros(n, dtype=bool)


def _signal_label(signal):
    codes = list(signal) if isinstance(signal, (list, tuple)) else [signal]
    names = [ENTRY_SIGNALS.get(c, c) for c in codes if c]
    return ' + '.join(names) if names else '—'


# 出場策略網格（type, 參數）
def _exit_strategy_grid():
    grid = []
    for sl, tp in [(0.10, 0.10), (0.10, 0.20), (0.08, 0.15), (0.05, 0.10)]:
        grid.append({'name': f'停損{int(sl*100)}% / 停利{int(tp*100)}%',
                     'type': 'sl_tp', 'sl': sl, 'tp': tp})
    grid.append({'name': '停損10% 不停利', 'type': 'sl_only', 'sl': 0.10})
    grid.append({'name': '停損15% 不停利', 'type': 'sl_only', 'sl': 0.15})
    for d in [20, 60, 120, 252]:
        label = '一年' if d == 252 else f'{d}天'
        grid.append({'name': f'固定持有{label}', 'type': 'fixed', 'hold': d})
    for tr in [0.08, 0.10, 0.15]:
        grid.append({'name': f'移動停利{int(tr*100)}%', 'type': 'trailing', 'trail': tr})
    for ma in [5, 10, 20]:
        grid.append({'name': f'跌破 MA{ma} 出場', 'type': 'ma_break', 'ma': ma})
    return grid


def _simulate_trades(hist, entry_mask, exit_cfg, max_hold=252):
    """依進場訊號與單一出場策略模擬交易，回傳交易報酬清單。不允許持倉重疊。"""
    close = hist['Close'].values
    high  = hist['High'].values
    low   = hist['Low'].values
    n     = len(close)
    trades = []
    etype  = exit_cfg['type']
    hold_limit = exit_cfg.get('hold', max_hold)
    # 跌破均線出場：先算好整段 MA 序列，收盤跌破即在當根收盤價出場
    ma_arr = None
    if etype == 'ma_break':
        ma_arr = hist['Close'].rolling(int(exit_cfg.get('ma', 5)), min_periods=1).mean().values

    i = 0
    while i < n - 1:
        if not entry_mask[i]:
            i += 1
            continue
        entry_price = close[i]
        if entry_price <= 0:
            i += 1
            continue
        peak = entry_price
        exit_price = None
        exit_idx   = None
        end = min(i + 1 + hold_limit, n)
        for j in range(i + 1, end):
            peak = max(peak, high[j])
            if etype == 'sl_tp':
                if low[j] <= entry_price * (1 - exit_cfg['sl']):
                    exit_price = entry_price * (1 - exit_cfg['sl']); exit_idx = j; break
                if high[j] >= entry_price * (1 + exit_cfg['tp']):
                    exit_price = entry_price * (1 + exit_cfg['tp']); exit_idx = j; break
            elif etype == 'sl_only':
                if low[j] <= entry_price * (1 - exit_cfg['sl']):
                    exit_price = entry_price * (1 - exit_cfg['sl']); exit_idx = j; break
            elif etype == 'trailing':
                if low[j] <= peak * (1 - exit_cfg['trail']):
                    exit_price = peak * (1 - exit_cfg['trail']); exit_idx = j; break
            elif etype == 'ma_break':
                if close[j] < ma_arr[j]:
                    exit_price = close[j]; exit_idx = j; break
            # fixed：不中途出場，等持有期滿
        if exit_price is None:
            exit_idx   = min(i + hold_limit, n - 1)
            exit_price = close[exit_idx]
        ret = (exit_price / entry_price - 1) * 100
        trades.append({'ret': round(ret, 2), 'hold': int(exit_idx - i)})
        i = exit_idx + 1   # 平倉後才找下一個進場點
    return trades


def _summarize(trades):
    """彙整一組交易的績效指標。"""
    if not trades:
        return {'trades': 0, 'win_rate': 0, 'avg_ret': 0, 'total_ret': 0,
                'avg_hold': 0, 'max_win': 0, 'max_loss': 0}
    rets  = [t['ret'] for t in trades]
    wins  = [r for r in rets if r > 0]
    # 複利總報酬（依序投入同一檔，非重疊）
    compounded = 1.0
    for r in rets:
        compounded *= (1 + r / 100)
    total_ret = (compounded - 1) * 100
    return {
        'trades':   len(trades),
        'win_rate': round(len(wins) / len(rets) * 100, 1),
        'avg_ret':  round(sum(rets) / len(rets), 2),
        'total_ret':round(total_ret, 1),
        'avg_hold': round(sum(t['hold'] for t in trades) / len(trades), 1),
        'max_win':  round(max(rets), 2),
        'max_loss': round(min(rets), 2),
    }


def _bt_load_history(ticker, period='3y'):
    """下載單一台股日線歷史；上市找不到自動退回上櫃。回傳 (hist, code, name) 或 (None, err, None)。"""
    code = ticker.replace('.TW', '').replace('.TWO', '')
    yf_ticker = tw_normalize(ticker)
    try:
        hist = yf.Ticker(yf_ticker).history(period=period, interval='1d')
        if hist.empty or len(hist) < 60:
            hist = yf.Ticker(code + '.TWO').history(period=period, interval='1d')
        if hist.empty or len(hist) < 60:
            return None, f'{ticker} 歷史資料不足，無法回測', None
    except Exception as e:
        return None, str(e), None
    return hist, code, tw_cn_name(code, code)


def _normalize_signal(signal):
    """把使用者/AI 傳入的訊號整理成乾淨清單，過濾未知代碼。"""
    codes = list(signal) if isinstance(signal, (list, tuple)) else [signal]
    codes = [c for c in codes if c in ENTRY_SIGNALS]
    return codes or ['kd_gc']


def _run_backtest(ticker, signal='kd_gc', period='3y'):
    """對單一台股跑進場訊號（可多條件 AND）+ 全出場策略網格回測，回傳排名結果。"""
    hist, code, name = _bt_load_history(ticker, period)
    if hist is None:
        return {'error': code}

    codes      = _normalize_signal(signal)
    entry_mask = _compute_entry_mask(hist, codes)
    n_signals  = int(entry_mask.sum())

    # 基準：買進並持有整段期間的報酬
    bh_ret = round((hist['Close'].iloc[-1] / hist['Close'].iloc[0] - 1) * 100, 1)

    results = []
    for cfg in _exit_strategy_grid():
        trades = _simulate_trades(hist, entry_mask, cfg)
        summ   = _summarize(trades)
        summ['strategy'] = cfg['name']
        results.append(summ)

    # 依複利總報酬排名
    results.sort(key=lambda x: x['total_ret'], reverse=True)

    return {
        'code':        code,
        'name':        name,
        'signal':      codes if len(codes) > 1 else codes[0],
        'signal_name': _signal_label(codes),
        'period':      period,
        'bars':        len(hist),
        'n_signals':   n_signals,
        'buy_hold_ret':bh_ret,
        'best':        results[0] if results else None,
        'results':     results,
    }


def _optimize_backtest(ticker, period='3y', min_trades=4):
    """『AI 幫我組最佳策略』核心：對一檔總當試 所有進場訊號 × 全出場策略，
    依複利總報酬排名，挑出最賺的『進場×出場』組合。資料只下載一次。"""
    hist, code, name = _bt_load_history(ticker, period)
    if hist is None:
        return {'error': code}

    bh_ret = round((hist['Close'].iloc[-1] / hist['Close'].iloc[0] - 1) * 100, 1)
    grid   = _exit_strategy_grid()
    combos = []
    for sig_code, sig_label in ENTRY_SIGNALS.items():
        if sig_code == 'buy_hold':
            continue
        mask   = _compute_entry_mask(hist, sig_code)
        n_sig  = int(mask.sum())
        if n_sig < min_trades:
            continue
        for cfg in grid:
            trades = _simulate_trades(hist, mask, cfg)
            summ   = _summarize(trades)
            if summ['trades'] < min_trades:    # 樣本太少不可靠，跳過
                continue
            summ.update({'entry_code': sig_code, 'entry': sig_label,
                         'strategy': cfg['name'], 'n_signals': n_sig})
            combos.append(summ)

    combos.sort(key=lambda x: x['total_ret'], reverse=True)
    return {
        'code': code, 'name': name, 'period': period,
        'bars': len(hist), 'buy_hold_ret': bh_ret,
        'tested': len(combos),
        'best': combos[0] if combos else None,
        'top':  combos[:12],
    }


# ═══════════════════════════════════════════════════════════════════════════
#  策略工作台：用「回測進場訊號」當選股條件（選股 ≡ 回測同一套指標）
# ═══════════════════════════════════════════════════════════════════════════

def _wb_cond_extra(code, hist, conditions):
    """為工作台籌碼/量能條件按需抓取 FinMind 資料（台股逐檔），回 extra dict。"""
    ctypes = {c.get('type', '') for c in conditions}
    extra  = {}
    if ctypes & _MARGIN_TYPES:
        mg = _get_tw_margin(code);          extra['margin']   = mg if mg else None
    if ctypes & _INST_TYPES:
        it = _get_tw_inst(code);            extra['inst']     = it if it else None
    if ctypes & _INST_HIST_TYPES:
        ih = _get_tw_inst_hist(code);       extra['inst_hist']= ih if ih else None
    if ctypes & _LENDING_TYPES:
        ld = _get_tw_lending(code);         extra['lending']  = ld if ld else None
    if ctypes & _HOLDING_TYPES:
        fh = _get_tw_foreign_holding(code); extra['holding']  = fh if fh else None
    if ctypes & _DAYTRADE_TYPES:
        dt = _get_tw_daytrade(code);        extra['daytrade'] = dt if dt else None
    return extra


def _scan_entry_signals(signal_codes, universe_key='top100',
                        price_min=0, price_max=99999, period='2y',
                        conditions=None) -> list:
    """對股池每檔，檢查進場訊號（最新K棒觸發）＋籌碼/量能篩選條件是否同時成立。
    進場訊號與回測同源；籌碼/量能為選股篩選層（不進回測，只縮小範圍）。"""
    codes = _normalize_signal(signal_codes) if signal_codes else []
    conditions = conditions or []
    universe = _get_tw_universe(universe_key)
    tickers  = list(dict.fromkeys(tw_normalize(c) for c in universe))

    def _check(t):
        try:
            hist, code, name = _bt_load_history(t, period)
            if hist is None or len(hist) < 30:
                return None
            price = float(hist['Close'].iloc[-1])
            if not (price_min <= price <= price_max):
                return None
            # 進場訊號：最新一根 K 棒須觸發（若有選）
            if codes:
                mask = _compute_entry_mask(hist, codes)
                if not (len(mask) and bool(mask[-1])):
                    return None
            # 籌碼/量能篩選條件：全部須成立（若有選）
            if conditions:
                extra = _wb_cond_extra(code, hist, conditions)
                for cond in conditions:
                    passed, _detail = _eval_condition(hist, {}, cond, extra)
                    if not bool(passed):
                        return None
            prev = float(hist['Close'].iloc[-2]) if len(hist) >= 2 else price
            change = round((price / prev - 1) * 100, 2) if prev else 0
            return {
                'code': code, 'name': name, 'price': round(price, 2),
                'changePct': change,
                'signal_date': hist.index[-1].strftime('%Y-%m-%d'),
            }
        except Exception:
            return None

    results = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(_check, t): t for t in tickers}
        for f in as_completed(futs):
            try:
                r = f.result()
                if r:
                    results.append(r)
            except Exception:
                pass
    results.sort(key=lambda x: x.get('changePct', 0), reverse=True)
    return results


def _workbench_batch_backtest(codes, signal_codes, exit_cfg, period='3y') -> dict:
    """對一籃股票用『同一進場訊號 + 同一買賣策略』逐檔回測，回每檔績效＋整籃彙總。"""
    sig = _normalize_signal(signal_codes)
    per_stock = []

    def _one(code):
        hist, c, name = _bt_load_history(code, period)
        if hist is None:
            return None
        mask   = _compute_entry_mask(hist, sig)
        trades = _simulate_trades(hist, mask, exit_cfg)
        summ   = _summarize(trades)
        bh = round((hist['Close'].iloc[-1] / hist['Close'].iloc[0] - 1) * 100, 1)
        summ.update({'code': c, 'name': name, 'n_signals': int(mask.sum()),
                     'buy_hold_ret': bh})
        return summ

    with ThreadPoolExecutor(max_workers=8) as ex:
        for r in ex.map(_one, codes):
            if r:
                per_stock.append(r)

    per_stock.sort(key=lambda x: x['total_ret'], reverse=True)
    n = len(per_stock)
    agg = {}
    if n:
        agg = {
            'count':      n,
            'avg_total':  round(sum(s['total_ret'] for s in per_stock) / n, 1),
            'avg_win':    round(sum(s['win_rate']  for s in per_stock) / n, 1),
            'avg_bh':     round(sum(s['buy_hold_ret'] for s in per_stock) / n, 1),
            'total_trades': sum(s['trades'] for s in per_stock),
            'beat_bh':    sum(1 for s in per_stock if s['total_ret'] > s['buy_hold_ret']),
        }
    return {
        'entry':    sig if len(sig) > 1 else sig[0],
        'entry_name': _signal_label(sig),
        'exit_name':  exit_cfg.get('name', exit_cfg.get('type', '')),
        'period':   period,
        'per_stock': per_stock,
        'summary':  agg,
    }


def _run_workbench_daily(cfg, client=None) -> dict:
    """每日收盤：用使用者存的工作台策略掃描股池＋回測前幾檔，推 LINE 報告。"""
    wb = cfg.get('workbench', {}) or {}
    if not wb.get('enabled') or not wb.get('signals'):
        return {}
    today = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d')
    pmax  = safe_float(wb.get('price_max', 0)) or 99999
    matched = _scan_entry_signals(
        wb['signals'], wb.get('universe', 'top100'),
        safe_float(wb.get('price_min', 0)), pmax,
        conditions=wb.get('conditions', []))
    top_codes = [m['code'] for m in matched[:10]]
    exit_cfg  = wb.get('exit') or _exit_strategy_grid()[0]
    bt = _workbench_batch_backtest(top_codes, wb['signals'], exit_cfg) if top_codes else {}

    name = wb.get('strategy_name') or _signal_label(_normalize_signal(wb['signals']))
    lines = [f'[{today}] 策略工作台掃描', f'策略：{name}',
             f'出場：{exit_cfg.get("name", "")}', '']
    if matched:
        lines.append(f'今日觸發 {len(matched)} 檔，前幾名：')
        for m in matched[:8]:
            lines.append(f"{m['name']}（{m['code']}） {m['price']} 元（{m['changePct']:+.1f}%）")
        s = bt.get('summary') or {}
        if s:
            lines.append('')
            lines.append(f"前 {s['count']} 檔歷史回測：平均總報酬 {s['avg_total']}%、"
                         f"平均勝率 {s['avg_win']}%、{s['beat_bh']}/{s['count']} 檔贏過買進持有。")
    else:
        lines.append('今日無符合此策略的標的。')

    cfg = _load_agent_cfg()
    wb = cfg.get('workbench', {}) or {}
    wb['last_run'] = today
    wb['last_result'] = {'date': today, 'matched': matched[:20], 'backtest': bt}
    cfg['workbench'] = wb
    _save_agent_cfg(cfg)
    _agent_notify(cfg, '策略工作台掃描', '\n'.join(lines))
    return wb['last_result']


def _tool_backtest(code, signal='kd_gc', period='3y'):
    """回測工具：回傳最優出場策略與排名（精簡給 AI）。"""
    r = _run_backtest(code, signal, period)
    if r.get('error'):
        return r
    return {
        'code': r['code'], 'name': r['name'],
        'entry_signal': r['signal_name'],
        'period': r['period'], 'signal_count': r['n_signals'],
        'buy_hold_return_pct': r['buy_hold_ret'],
        'best_strategy': r['best'],
        'top5': r['results'][:5],
    }


# ═══════════════════════════════════════════════════════════════════════════
#  每日策略優化器（自動組合條件 → 篩選 → 回測 → 排名 → AI 總結）
# ═══════════════════════════════════════════════════════════════════════════

# 經人工檢核的策略配方庫（條件組合 + 對應可回測的進場訊號）
STRATEGY_RECIPES = [
    {'name': '法人連買起漲', 'entry_signal': 'ma_gc',
     'conditions': [{'type': 'inst_3_buy_ndays', 'params': {'days': 3}},
                    {'type': 'ma_deduction_up'}, {'type': 'vol_5_above_20'}]},
    {'name': '低檔KD金叉反彈', 'entry_signal': 'kd_gc',
     'conditions': [{'type': 'kd_low_golden_cross', 'params': {'threshold': 35}},
                    {'type': 'price_near_bb_lower'}]},
    {'name': '強勢突破帶量', 'entry_signal': 'breakout_ma20',
     'conditions': [{'type': 'high_vol_breakout'}, {'type': 'ma_bull_alignment'},
                    {'type': 'macd_golden_cross'}]},
    {'name': '外資連買均線多頭', 'entry_signal': 'ma_gc',
     'conditions': [{'type': 'inst_foreign_buy_ndays', 'params': {'days': 3}},
                    {'type': 'ma_bull_alignment'}]},
    {'name': '月線扣抵翻揚', 'entry_signal': 'kd_gc',
     'conditions': [{'type': 'ma_deduction_up'}, {'type': 'kd_golden_cross'},
                    {'type': 'vol_5_above_20'}]},
]


def _optimizer_ai_summary(client, ranked):
    """讓 Claude 對當日策略排名寫一段簡短分析。"""
    lines = ['以下是今日各策略配方的回測表現排名，請用 3-5 句話總結哪個策略現在最值得用、為什麼，並點出最佳標的：', '']
    for r in ranked:
        picks = '、'.join(f"{p['name']}({p['code']})" for p in r['top_picks']) or '無符合'
        lines.append(f"策略「{r['name']}」：符合{r['match_count']}檔，回測平均報酬{r['avg_backtest_ret']}%，候選：{picks}")
    try:
        resp = client.messages.create(
            model='claude-opus-4-8', max_tokens=500,
            system=_AGENT_SYSTEM_PROMPT,
            messages=[{'role': 'user', 'content': '\n'.join(lines)}],
        )
        return resp.content[0].text
    except Exception as e:
        return f'AI 總結失敗：{e}'


def _run_daily_optimizer(cfg, client):
    """每日跑策略配方庫：篩選 + 回測候選 + 排名 + AI 總結，存檔並推播。"""
    today = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d')
    universe = _get_tw_universe('top100')[:60]   # 限制成本
    tickers  = list(dict.fromkeys(tw_normalize(c) for c in universe))

    ranked = []
    for recipe in STRATEGY_RECIPES:
        conds = recipe['conditions']
        matched = []
        with ThreadPoolExecutor(max_workers=12) as ex:
            futs = {ex.submit(_scan_ticker, t, conds, True): t for t in tickers}
            for f in as_completed(futs):
                try:
                    r = f.result()
                    if r:
                        matched.append(r)
                except Exception:
                    pass
        matched.sort(key=lambda x: x.get('changePct', 0), reverse=True)
        top = matched[:3]

        bt_rets = []
        for m in top:
            code = m['ticker'].replace('.TW', '').replace('.TWO', '')
            bt = _run_backtest(code, recipe['entry_signal'], '3y')
            if not bt.get('error') and bt.get('best'):
                bt_rets.append(bt['best']['total_ret'])
        avg_bt = round(sum(bt_rets) / len(bt_rets), 1) if bt_rets else 0

        ranked.append({
            'name':          recipe['name'],
            'entry_signal':  ENTRY_SIGNALS.get(recipe['entry_signal'], recipe['entry_signal']),
            'match_count':   len(matched),
            'avg_backtest_ret': avg_bt,
            'top_picks':     [{'code': m['ticker'].replace('.TW', '').replace('.TWO', ''),
                               'name': m['name'], 'price': m['price']} for m in top],
        })

    # 先有選到標的的策略才排前面，再比回測平均報酬；全空窗時不會硬挑一個假最佳
    ranked.sort(key=lambda x: (x['match_count'] > 0, x['avg_backtest_ret']), reverse=True)
    summary = _optimizer_ai_summary(client, ranked)

    cfg = _load_agent_cfg()
    cfg['daily_strategy'] = {'date': today, 'ranked': ranked, 'summary': summary}
    cfg['last_optimize']  = today
    _save_agent_cfg(cfg)

    # 只有真的選到標的的策略才能當「最佳」；否則視為訊號空窗
    best = next((r for r in ranked if r['match_count'] > 0), None)
    body_lines = [f'[{today}] 每日最佳策略優化', '']
    if best:
        picks = '、'.join(f"{p['name']}({p['code']})" for p in best['top_picks']) or '無'
        body_lines.append(f"今日最佳策略：{best['name']}")
        body_lines.append(f"進場訊號：{best['entry_signal']}")
        body_lines.append(f"符合 {best['match_count']} 檔，回測平均報酬 {best['avg_backtest_ret']}%")
        body_lines.append(f"候選標的：{picks}")
        body_lines.append('')
    else:
        body_lines.append('今日五大策略皆無標的通過篩選（訊號空窗），建議空手觀望，'
                          '不在此時硬挑「最佳策略」。')
        body_lines.append('')
    body_lines.append(summary[:400])
    _agent_notify(cfg, '每日策略優化', '\n'.join(body_lines))
    return ranked


def _dispatch_tool(name, tool_input):
    """執行工具並回傳結果 dict。"""
    try:
        if name == 'screen_stocks':
            return _tool_screen_stocks(
                conditions=tool_input.get('conditions', []),
                scope=tool_input.get('scope', 'sector'),
                sector=tool_input.get('sector', ''),
                price_min=tool_input.get('price_min', 0),
                price_max=tool_input.get('price_max', 9999))
        if name == 'analyze_stock':
            return _tool_analyze_stock(tool_input.get('code', ''))
        if name == 'get_stock_news':
            return _tool_get_news(tool_input.get('query', ''))
        if name == 'get_holdings':
            return _tool_get_holdings()
        if name == 'backtest_strategy':
            return _tool_backtest(tool_input.get('code', ''),
                                  tool_input.get('signal', 'kd_gc'),
                                  tool_input.get('period', '3y'))
        return {'error': f'未知工具 {name}'}
    except Exception as e:
        return {'error': str(e)}


def _tool_status_msg(name, tool_input):
    """產生給前端顯示的工具執行狀態文字。"""
    if name == 'screen_stocks':
        scope = tool_input.get('scope', 'sector')
        sector = tool_input.get('sector', '')
        where = f'{sector}類股' if scope == 'sector' and sector else '全市場'
        conds = '、'.join(c.get('type', '') for c in tool_input.get('conditions', [])[:4])
        return f'正在篩選{where}（{conds}）…'
    if name == 'analyze_stock':
        return f'正在深入分析 {tool_input.get("code", "")}…'
    if name == 'get_stock_news':
        return f'正在查詢 {tool_input.get("query", "")} 的相關新聞…'
    if name == 'get_holdings':
        return '正在讀取你的持倉部位…'
    if name == 'backtest_strategy':
        return f'正在回測 {tool_input.get("code", "")} 的最佳出場策略…'
    return '處理中…'


@app.route('/ai')
def ai_home():
    return render_template('ai_home.html')


@app.route('/api/ai/chat', methods=['POST'])
def ai_chat():
    """統一 AI 對話 — Claude Tool Use 編排迴圈（SSE 串流）。"""
    import anthropic
    from flask import Response, stream_with_context

    body    = request.get_json(force=True)
    messages = body.get('messages', [])
    scope_hint = body.get('scope_hint', '')   # 'sector' / 'market' / ''
    cfg     = _load_agent_cfg()
    api_key = body.get('api_key', '').strip() or cfg.get('claude_api_key', '')

    if not api_key:
        return jsonify({'error': '未設定 Claude API Key，請至 AI Agent 頁面設定'}), 400
    if not messages:
        return jsonify({'error': '訊息不可為空'}), 400

    system_prompt = _AI_SYSTEM_PROMPT
    if scope_hint == 'sector':
        system_prompt += '\n\n使用者偏好：優先以「指定產業」方式篩選（scope=sector）。'
    elif scope_hint == 'market':
        system_prompt += '\n\n使用者偏好：優先以「全市場」方式篩選（scope=market）。'

    def generate():
        try:
            client = anthropic.Anthropic(api_key=api_key)
            convo  = list(messages)
            all_cards = []

            for _ in range(6):  # 最多 6 輪工具呼叫，防止無限迴圈
                resp = client.messages.create(
                    model='claude-opus-4-8',
                    max_tokens=2000,
                    system=[{'type': 'text', 'text': system_prompt,
                             'cache_control': {'type': 'ephemeral'}}],
                    tools=_AI_TOOLS,
                    messages=convo,
                )

                # 串流輸出本輪的文字內容（去除 emoji 以符合使用者偏好）
                text_blocks = [b for b in resp.content if b.type == 'text']
                for tb in text_blocks:
                    if tb.text:
                        yield f"data: {json.dumps({'text': _strip_emoji(tb.text)})}\n\n"

                if resp.stop_reason != 'tool_use':
                    break

                # 處理工具呼叫
                convo.append({'role': 'assistant', 'content': resp.content})
                tool_results = []
                for block in resp.content:
                    if block.type != 'tool_use':
                        continue
                    status = _tool_status_msg(block.name, block.input)
                    yield f"data: {json.dumps({'status': status})}\n\n"

                    result = _dispatch_tool(block.name, block.input)

                    # 收集股票卡片給前端渲染
                    if block.name == 'screen_stocks' and result.get('matches'):
                        all_cards.extend(result['matches'])
                    if block.name == 'analyze_stock' and not result.get('error'):
                        all_cards.append(result)

                    tool_results.append({
                        'type': 'tool_result',
                        'tool_use_id': block.id,
                        'content': json.dumps(result, ensure_ascii=False),
                    })
                convo.append({'role': 'user', 'content': tool_results})

            if all_cards:
                # 去重（依 code）
                seen, uniq = set(), []
                for c in all_cards:
                    if c.get('code') and c['code'] not in seen:
                        seen.add(c['code'])
                        uniq.append(c)
                yield f"data: {json.dumps({'cards': uniq[:15]})}\n\n"

            yield "data: [DONE]\n\n"
        except anthropic.AuthenticationError:
            yield f"data: {json.dumps({'error': 'API Key 無效，請重新確認'})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@app.route('/backtest')
def backtest_page():
    return render_template('backtest.html')


@app.route('/goal')
def goal_page():
    return render_template('goal.html')


@app.route('/api/backtest/signals')
def backtest_signals():
    # groups：分組供前端勾選；flat：代碼→名稱對照（相容舊用法）
    return jsonify({'groups': SIGNAL_GROUPS_PUBLIC, 'flat': ENTRY_SIGNALS})


@app.route('/api/backtest/run', methods=['POST'])
def backtest_run():
    body   = request.get_json(force=True) or {}
    code   = str(body.get('code', '')).strip().upper().replace('.TW', '').replace('.TWO', '')
    # 支援單一 signal 或多條件 signals 清單（逐根 AND）
    signal = body.get('signals') or body.get('signal') or 'kd_gc'
    period = body.get('period', '3y')
    if not code:
        return jsonify({'error': '請輸入股票代碼'}), 400
    result = _run_backtest(code, signal, period)
    return jsonify(result)


@app.route('/api/backtest/optimize', methods=['POST'])
def backtest_optimize():
    """AI 幫我組最佳策略：總當試所有進場×出場組合，回傳最賺的組合排名。"""
    body   = request.get_json(force=True) or {}
    code   = str(body.get('code', '')).strip().upper().replace('.TW', '').replace('.TWO', '')
    period = body.get('period', '3y')
    if not code:
        return jsonify({'error': '請輸入股票代碼'}), 400
    return jsonify(_optimize_backtest(code, period))


@app.route('/api/backtest/advice', methods=['POST'])
def backtest_advice():
    """對既有回測/最佳化結果，由 Claude 寫出文字操作建議與預期報酬（不重算）。"""
    import anthropic
    body    = request.get_json(force=True) or {}
    cfg     = _load_agent_cfg()
    api_key = (body.get('api_key', '') or '').strip() or _agent_api_key(cfg)
    if not api_key:
        return jsonify({'error': '未設定 Claude API Key，請至 AI Agent 頁面設定'}), 400

    name   = body.get('name', body.get('code', ''))
    code   = body.get('code', '')
    period = body.get('period', '3y')
    bh     = body.get('buy_hold_ret', 0)
    best   = body.get('best') or {}
    rows   = body.get('top') or body.get('results') or []
    mode   = body.get('mode', 'run')   # 'run' 單一訊號 / 'optimize' 最佳化

    lines = [f'標的：{name}（{code}），回測期間 {period}，買進持有基準報酬 {bh}%。']
    if mode == 'optimize':
        lines.append(f'最佳組合：進場「{best.get("entry","-")}」+ 出場「{best.get("strategy","-")}」。')
    else:
        lines.append(f'進場訊號：{body.get("signal_name","-")}，訊號出現 {body.get("n_signals","-")} 次。')
        lines.append(f'最優出場策略：{best.get("strategy","-")}。')
    lines.append(f'該組合複利總報酬 {best.get("total_ret","-")}%、勝率 {best.get("win_rate","-")}%、'
                 f'平均每筆 {best.get("avg_ret","-")}%、交易 {best.get("trades","-")} 次、'
                 f'平均持有 {best.get("avg_hold","-")} 天、最大單筆獲利 {best.get("max_win","-")}%、'
                 f'最大單筆虧損 {best.get("max_loss","-")}%。')
    lines.append('\n其他組合摘要：')
    for r in rows[:6]:
        tag = f"{r.get('entry','')}+{r['strategy']}" if r.get('entry') else r.get('strategy', '')
        lines.append(f"- {tag}：總報酬{r.get('total_ret','-')}% 勝率{r.get('win_rate','-')}% 交易{r.get('trades','-')}次")
    lines.append(
        '\n請用繁體中文、不要用 emoji，3-5 句話給出：'
        '(1) 這檔最適合的操作方式（進場時機＋該配哪種停損停利/持有方式）；'
        '(2) 依勝率與平均每筆估算的「單次預期報酬」白話說明；'
        '(3) 風險提醒（樣本數、過度最佳化、過去不代表未來）。'
    )
    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model='claude-opus-4-8', max_tokens=600,
            system=_AGENT_SYSTEM_PROMPT,
            messages=[{'role': 'user', 'content': '\n'.join(lines)}],
        )
        return jsonify({'advice': _strip_emoji(resp.content[0].text)})
    except anthropic.AuthenticationError:
        return jsonify({'error': 'API Key 無效'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
#  策略工作台（選股 → 買賣策略 → 回測 一條龍）
# ═══════════════════════════════════════════════════════════════════════════

# 工作台選股可加的「籌碼/量能篩選條件」（選股時套用，縮小範圍；不進回測）。
# 只列免費 FinMind 取得到的；大戶持股級距、分點券商需 FinMind 付費 sponsor，暫不列。
WORKBENCH_CONDITION_GROUPS = [
    {'name': '趨勢／均線（狀態）', 'conditions': [
        {'type': 'price_above_ma', 'label': '股價站上 N 日均線（站上即算，非剛突破）',
         'params': [{'key': 'period', 'label': 'MA', 'default': 60}]},
        {'type': 'ma_trending_up', 'label': 'N 日均線向上彎',
         'params': [{'key': 'period', 'label': 'MA', 'default': 20}]},
        {'type': 'ma_bull_alignment', 'label': '均線多頭排列（5>10>20>60）', 'params': []},
        {'type': 'price_from_high_below', 'label': '距 52 週高點回檔 < N%',
         'params': [{'key': 'threshold', 'label': '%', 'default': 20}]},
        {'type': 'rsi_below', 'label': 'RSI 低於 N（超賣）',
         'params': [{'key': 'threshold', 'label': 'RSI', 'default': 30}]},
        {'type': 'kd_k_below', 'label': 'KD 的 K 值低於 N（低檔）',
         'params': [{'key': 'kd_n', 'label': 'KD週期', 'default': 9},
                    {'key': 'threshold', 'label': 'K', 'default': 20}]},
        {'type': 'kd_k_above', 'label': 'KD 的 K 值高於 N（強勢／中軸之上）',
         'params': [{'key': 'kd_n', 'label': 'KD週期', 'default': 60},
                    {'key': 'threshold', 'label': 'K', 'default': 50}]},
    ]},
    {'name': '量能', 'conditions': [
        {'type': 'volume_ratio_above', 'label': '爆量（量 > N 倍均量）',
         'params': [{'key': 'ratio', 'label': '倍數', 'default': 1.5}]},
        {'type': 'vol5_avg_above', 'label': '5 日均量大於 N 張',
         'params': [{'key': 'threshold', 'label': '張數', 'default': 1000}]},
        {'type': 'volume_5d_high', 'label': '成交量創 5 日新高', 'params': []},
        {'type': 'vol_5_above_20', 'label': '量能轉強（5 日均量站上 20 日）', 'params': []},
        {'type': 'volume_shrinking', 'label': '量縮（賣壓宣洩）', 'params': []},
        {'type': 'high_vol_breakout', 'label': '帶量突破前高', 'params': []},
        {'type': 'day_trade_ratio_above', 'label': '當沖比大於 N%',
         'params': [{'key': 'threshold', 'label': '%', 'default': 50}]},
    ]},
    {'name': '法人籌碼', 'conditions': [
        {'type': 'inst_foreign_buy_ndays', 'label': '外資連續 N 日買超',
         'params': [{'key': 'days', 'label': '天數', 'default': 3}]},
        {'type': 'inst_trust_buy_ndays', 'label': '投信連續 N 日買超',
         'params': [{'key': 'days', 'label': '天數', 'default': 3}]},
        {'type': 'inst_3_buy_ndays', 'label': '三大法人連續 N 日買超',
         'params': [{'key': 'days', 'label': '天數', 'default': 3}]},
        {'type': 'inst_net_sum_above', 'label': '近 N 日法人累計買超 > X 千股',
         'params': [{'key': 'days', 'label': '天數', 'default': 5},
                    {'key': 'threshold', 'label': '千股', 'default': 5000}]},
        {'type': 'inst_total_above', 'label': '法人今日合計買超 > N 千股',
         'params': [{'key': 'threshold', 'label': '千股', 'default': 1000}]},
        {'type': 'foreign_holding_above', 'label': '外資持股比例大於 N%',
         'params': [{'key': 'threshold', 'label': '%', 'default': 10}]},
        {'type': 'inst_pct_above', 'label': '機構持股比例大於 N%',
         'params': [{'key': 'threshold', 'label': '%', 'default': 20}]},
    ]},
    {'name': '資券/借券', 'conditions': [
        {'type': 'margin_decrease', 'label': '融資減少（散戶退場）', 'params': []},
        {'type': 'margin_increase', 'label': '融資增加', 'params': []},
        {'type': 'short_decrease', 'label': '融券減少（空方回補）', 'params': []},
        {'type': 'high_short_ratio', 'label': '高券資比（大於 N%）',
         'params': [{'key': 'threshold', 'label': '%', 'default': 30}]},
        {'type': 'lending_decrease', 'label': '借券餘額減少（空方回補）', 'params': []},
    ]},
]


@app.route('/workbench')
def workbench_page():
    return render_template('workbench.html')


@app.route('/api/workbench/catalog')
def workbench_catalog():
    """進場指標分組（同回測訊號）＋籌碼/量能篩選條件＋出場策略＋股池選項，供前端建表。"""
    return jsonify({
        'signal_groups':    SIGNAL_GROUPS_PUBLIC,
        'condition_groups': WORKBENCH_CONDITION_GROUPS,
        'exits':            _exit_strategy_grid(),
        'universes':        [
            {'key': 'top50',  'label': '成交量前 50 大'},
            {'key': 'top100', 'label': '成交量前 100 大'},
            {'key': 'top200', 'label': '成交量前 200 大'},
        ],
    })


@app.route('/api/workbench/scan', methods=['POST'])
def workbench_scan():
    """以進場訊號＋籌碼/量能條件掃描股池，回傳今天符合的股票（= 選股）。"""
    body       = request.get_json(force=True) or {}
    signals    = body.get('signals', [])
    conditions = body.get('conditions', []) or []
    if not signals and not conditions:
        return jsonify({'error': '請至少選一個進場指標或籌碼/量能條件'}), 400
    universe = body.get('universe', 'top100')
    pmin     = safe_float(body.get('price_min', 0))
    pmax     = safe_float(body.get('price_max', 99999)) or 99999
    try:
        results = _scan_entry_signals(signals, universe, pmin, pmax, conditions=conditions)
        name = _signal_label(_normalize_signal(signals)) if signals else '（純籌碼/量能篩選）'
        return jsonify({'results': results, 'matched': len(results), 'signal_name': name})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': f'掃描錯誤：{str(e)[:80]}'}), 500


@app.route('/api/workbench/ai_strategy', methods=['POST'])
def workbench_ai_strategy():
    """AI 從進場指標目錄挑一組策略（含建議出場），列出指標與理由給使用者確認。"""
    import anthropic
    cfg     = _load_agent_cfg()
    body    = request.get_json(force=True) or {}
    api_key = (body.get('api_key', '') or '').strip() or _agent_api_key(cfg)
    if not api_key:
        return jsonify({'error': '未設定 Claude API Key，請至 AI Agent 頁面設定'}), 400

    catalog = '\n'.join(
        f"- {code}：{label}"
        for _g, items in _SIGNAL_DEFS for code, label, _fn in items)
    cond_catalog = '\n'.join(
        f"- {c['type']}：{c['label']}"
        for g in WORKBENCH_CONDITION_GROUPS for c in g['conditions'])
    exits = '\n'.join(f"- {e['name']}" for e in _exit_strategy_grid())
    prompt = (
        '你是台股策略設計師。請設計一個「起漲選股 + 籌碼/量能確認」的策略：\n'
        '1) 從「可用進場指標」挑 2 到 4 個能互相搭配、邏輯一致的技術指標（可回測）。\n'
        '2) 從「可用籌碼/量能條件」挑 0 到 3 個最能加強這個策略勝率的條件當篩選（例如法人連買、量能轉強、'
        '當沖比過高要避開等），並說明為何這些較重要。\n'
        '3) 從「可用出場策略」挑一個最搭的買賣出場規則。\n\n'
        f'可用進場指標（代碼：說明）：\n{catalog}\n\n'
        f'可用籌碼/量能條件（代碼：說明）：\n{cond_catalog}\n\n'
        f'可用出場策略：\n{exits}\n\n'
        '只能從上面清單挑，不可自創代碼。請只回傳 JSON，格式：\n'
        '{"signal_codes": ["代碼1","代碼2"], "condition_codes": ["條件代碼1"], '
        '"exit_name": "完整出場策略名稱", "strategy_name": "策略短名", '
        '"reason": "為何這樣組，含挑這些籌碼/量能條件的重要性（繁體中文，3-4 句，不要 emoji）"}'
    )
    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model='claude-opus-4-8', max_tokens=700,
            messages=[{'role': 'user', 'content': prompt}],
        )
        text = resp.content[0].text.strip()
        m = _re.search(r'\{.*\}', text, _re.S)
        data = json.loads(m.group(0)) if m else {}
        codes = _normalize_signal(data.get('signal_codes', []))
        exit_name = data.get('exit_name', '')
        exit_cfg = next((e for e in _exit_strategy_grid() if e['name'] == exit_name), None)
        # 把 AI 挑的條件代碼對回工作台條件目錄（含預設參數）
        cond_map = {c['type']: c for g in WORKBENCH_CONDITION_GROUPS for c in g['conditions']}
        chosen_conds = []
        for ct in (data.get('condition_codes', []) or []):
            c = cond_map.get(ct)
            if c:
                chosen_conds.append({
                    'type': ct, 'label': c['label'],
                    'params': {p['key']: p['default'] for p in c.get('params', [])},
                })
        return jsonify({
            'signal_codes': codes,
            'signal_name':  _signal_label(codes),
            'conditions':   chosen_conds,
            'exit':         exit_cfg,
            'strategy_name': data.get('strategy_name', ''),
            'reason':       _strip_emoji(data.get('reason', '')),
        })
    except anthropic.AuthenticationError:
        return jsonify({'error': 'API Key 無效'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/workbench/backtest', methods=['POST'])
def workbench_backtest():
    """對選股命中的一籃股票，用同一進場訊號＋同一買賣策略批次回測。"""
    body    = request.get_json(force=True) or {}
    codes   = [str(c).strip().upper().replace('.TW', '').replace('.TWO', '')
               for c in body.get('codes', []) if str(c).strip()]
    signals = body.get('signals', [])
    exit_cfg = body.get('exit') or {}
    period  = body.get('period', '3y')
    if not codes:
        return jsonify({'error': '沒有要回測的股票'}), 400
    if not signals:
        return jsonify({'error': '缺少進場指標'}), 400
    if not exit_cfg.get('type'):
        return jsonify({'error': '請選擇買賣（出場）策略'}), 400
    codes = list(dict.fromkeys(codes))[:25]   # 上限保護
    try:
        return jsonify(_workbench_batch_backtest(codes, signals, exit_cfg, period))
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': f'回測錯誤：{str(e)[:80]}'}), 500


@app.route('/api/workbench/config', methods=['GET', 'POST'])
def workbench_config_api():
    """讀取/儲存工作台 AI 策略（供每日自動掃描使用）。"""
    cfg = _load_agent_cfg()
    if request.method == 'GET':
        return jsonify(cfg.get('workbench', {}))
    body = request.get_json(force=True) or {}
    wb = cfg.get('workbench', {}) or {}
    for k in ('enabled', 'signals', 'conditions', 'exit', 'universe', 'price_min', 'price_max',
              'strategy_name'):
        if k in body:
            wb[k] = body[k]
    cfg['workbench'] = wb
    _save_agent_cfg(cfg)
    return jsonify({'ok': True, 'workbench': wb})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6001, debug=False)

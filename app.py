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
from concurrent.futures import ThreadPoolExecutor, as_completed
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

def _load_tw_names():
    """抓取全上市櫃中文名稱並快取。
    註：openapi.twse.com.tw 會回安全擋頁（非 JSON），改用實際可用來源：
      上市 www.twse.com.tw rwd JSON、上櫃 www.tpex.org.tw openapi。"""
    global _tw_name_cache
    import urllib.request
    def _get_json(url):
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        return json.loads(urllib.request.urlopen(req, timeout=15).read())

    result = {}
    # 上市（TWSE）：data 列為 [證券代號, 證券名稱, …]
    try:
        d = _get_json('https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?response=json')
        for row in (d.get('data') or []):
            if len(row) >= 2 and row[0] and row[1]:
                result[str(row[0]).strip()] = str(row[1]).strip()
    except Exception:
        pass
    # 上櫃（TPEX）：{SecuritiesCompanyCode, CompanyName, …}
    try:
        for item in _get_json('https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes'):
            code = (item.get('SecuritiesCompanyCode') or item.get('Code') or '').strip()
            name = (item.get('CompanyName') or item.get('Name') or '').strip()
            if code and name:
                result[code] = name
    except Exception:
        pass

    if result:
        with _tw_name_lock:
            _tw_name_cache = result

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

def _push_line_msg(token, user_id, text):
    try:
        _requests.post(
            'https://api.line.me/v2/bot/message/push',
            headers={'Content-Type': 'application/json',
                     'Authorization': f'Bearer {token}'},
            json={'to': user_id, 'messages': [{'type': 'text', 'text': text}]},
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

def _run_server_scan():
    with _monitor_lock:
        cfg = _load_monitor_cfg()
    tickers_cfg = cfg.get('tickers', {})
    if not tickers_cfg:
        return
    now_str = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M')
    for ticker, settings in list(tickers_cfg.items()):
        try:
            profile = settings.get('profile', 'aggressive')
            stock = yf.Ticker(ticker)
            info = stock.info
            price = safe_float(info.get('currentPrice', info.get('regularMarketPrice', 0)))
            if price <= 0:
                continue
            name = info.get('shortName', info.get('longName', ticker))
            result = (_aggressive_signal(stock, ticker, price, name)
                      if profile == 'aggressive'
                      else _steady_signal(stock, ticker, price, name))
            action = result.get('action', 'WAIT')
            with _monitor_lock:
                cfg2 = _load_monitor_cfg()
                if ticker not in cfg2['tickers']:
                    continue
                cfg2['tickers'][ticker]['last_signal'] = result
                cfg2['tickers'][ticker]['last_scan'] = now_str
                entry        = cfg2['tickers'][ticker]
                line_token   = entry.get('line_token', '')
                line_user_id = entry.get('line_user_id', '')
                last_notify  = entry.get('last_notify_time', '')
                cooldown_ok  = (not last_notify or
                    (pd.Timestamp.now(tz='Asia/Taipei') -
                     pd.Timestamp(last_notify, tz='Asia/Taipei')).total_seconds() > 1800)
                if action == 'BUY' and line_token and line_user_id and cooldown_ok:
                    cfg2['tickers'][ticker]['last_notify_time'] = now_str
                    _save_monitor_cfg(cfg2)
                    _push_line_msg(line_token, line_user_id, _build_line_text(result))
                else:
                    _save_monitor_cfg(cfg2)
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

# ── TTL Cache ─────────────────────────────────────────────────────────
_CACHE = {}

def _cache_get(key):
    e = _CACHE.get(key)
    return e['v'] if e and time.time() < e['t'] else None

def _cache_set(key, val, ttl=300):
    _CACHE[key] = {'v': val, 't': time.time() + ttl}

# ── Helpers ───────────────────────────────────────────────────────
def safe_float(v, default=0.0):
    try:
        if v is None: return default
        f = float(v)
        return default if (np.isnan(f) or np.isinf(f)) else f
    except:
        return default

def last_valid(series, default=0.0):
    """Return last non-NaN value from a pandas Series (handles today's NaN for TW stocks)."""
    try:
        s = series.dropna()
        return safe_float(s.iloc[-1]) if len(s) else default
    except:
        return default

def safe_int(v, default=0):
    try:
        return int(safe_float(v))
    except:
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

    near_support  = long_min * 0.97 <= price <= long_max * 1.08
    above_support = price > long_min
    broke_support = price < long_min * 0.97

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

    if (near_support or above_support) and (golden_cross or macd_positive) and vol_shrink:
        action, action_cn = 'BUY', '安全打底！逢低佈局'
        conf = '高' if (golden_cross and vol_shrink) else '中'
        reason = (f'日線回測 GMMA 長期均線支撐（{long_min:.2f}~{long_max:.2f} 元）不破，'
                  f'量縮打底，MACD {"出現黃金交叉" if golden_cross else "底部轉強"}，'
                  f'適合做中長線的資金投入。')
    elif near_support and (macd_turning or oversold):
        action, action_cn = 'WATCH', '接近支撐！持續觀察'
        conf = '低'
        reason = (f'股價逼近 GMMA 長期均線支撐區（{long_min:.2f}~{long_max:.2f} 元）。'
                  f'{"RSI " + str(round(rsi_val, 0)) + " 超賣，" if oversold else ""}'
                  f'MACD 底部出現轉強跡象，若量縮確認後可逢低佈局。')
    elif broke_support:
        action, action_cn = 'AVOID', '趨勢偏弱，暫時迴避'
        conf = '-'
        reason = (f'股價跌破 GMMA 長期均線支撐（{long_min:.2f} 元），趨勢轉弱。'
                  f'建議等待重新站回長期均線後再考慮進場。')
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
        except:
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
        except:
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
        except:
            pass

        # ── Earnings date ──
        earnings_date = None
        try:
            cal = stock.calendar
            if cal is not None and not cal.empty:
                col = cal.columns[0]
                earnings_date = str(col.date()) if hasattr(col, 'date') else str(col)[:10]
        except:
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
        except:
            pass

        def clean(lst):
            res = []
            for x in lst:
                try:
                    f = float(x)
                    res.append(None if (np.isnan(f) or np.isinf(f)) else round(f, 4))
                except:
                    res.append(None)
            return res

        dates = hist.index.strftime('%Y-%m-%d').tolist()

        result = {
            'ticker':       ticker,
            'name':         info.get('longName', info.get('shortName', ticker)),
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
        except:
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
        except:
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
                except:
                    res.append(None)
            return res

        dates = hist.index.strftime('%Y-%m-%d').tolist()
        result = {
            'ticker':        ticker,
            'displayTicker': tw_display(ticker),
            'name':          info.get('longName', info.get('shortName', ticker)),
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
        except:
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
        except:
            pass

        earnings_date = None
        try:
            cal = stock.calendar
            if cal is not None and not cal.empty:
                col = cal.columns[0]
                earnings_date = str(col.date()) if hasattr(col, 'date') else str(col)[:10]
        except:
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
        except:
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
        except:
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
                except:
                    locals()[attr] = None

        if last_div_date:
            try: last_div_date = str(pd.Timestamp(last_div_date, unit='s').date())
            except: last_div_date = None
        if ex_div_date:
            try: ex_div_date = str(pd.Timestamp(ex_div_date, unit='s').date())
            except: ex_div_date = None

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
        _cache_set(f'tw_rt:{ticker}', result, ttl=30)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


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
    return jsonify(cfg.get('tickers', {}))


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
                except:
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
                except:
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


# ── TWSE 三大法人快取 ──────────────────────────────────────────────────
_tw_inst_cache: dict = {}
_tw_inst_lock  = threading.Lock()

def _load_tw_inst():
    """抓 TWSE 今日三大法人資料，快取 1 小時"""
    import urllib.request, datetime
    try:
        today = datetime.date.today().strftime('%Y%m%d')
        url = f'https://www.twse.com.tw/rwd/zh/fund/T86?date={today}&selectType=ALLBUT0999&response=json'
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
    with _tw_inst_lock:
        ts = _tw_inst_cache.get('ts', 0)
        data = _tw_inst_cache.get('data', {})
    if time.time() - ts > 3600 or not data:
        data = _load_tw_inst()
    return data.get(code)


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
        url = f'https://www.twse.com.tw/rwd/zh/fund/T86?date={ymd}&selectType=ALLBUT0999&response=json'
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
    """回傳單一股票的多日法人序列 dict（最新在前），找不到回 None。"""
    with _tw_inst_hist_lock:
        ts   = _tw_inst_hist_cache.get('ts', 0)
        data = _tw_inst_hist_cache.get('data', {})
    if time.time() - ts > 14400 or not data:
        data = _load_tw_inst_history()
    return data.get(code)


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
    """回傳單一股票融資融券資料（dict），找不到則 None"""
    with _tw_margin_lock:
        ts = _tw_margin_cache.get('ts', 0)
        data = _tw_margin_cache.get('data', {})
    if time.time() - ts > 3600 or not data:
        data = _load_tw_margin()
    return data.get(code)


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
                except: pass
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
                except: pass
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
                    except: pass
                return False, f'週KD({kn}) 無金叉 K={safe_float(k.iloc[-1]):.1f}'

            elif ctype == 'weekly_macd_golden_cross':
                if wh is None or len(wh) < 30: return False, '週線資料不足'
                within = int(params.get('within_days', 3))
                wm, ws, _ = calc_macd(wh['Close'])
                for i in range(-within, 0):
                    try:
                        if wm.iloc[i-1] < ws.iloc[i-1] and wm.iloc[i] > ws.iloc[i]:
                            return True, f'週MACD金叉 DIF={safe_float(wm.iloc[-1]):.2f}'
                    except: pass
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
                    except: pass
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
    now_str = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M')
    for ticker, entry in list(cfg.get('tickers', {}).items()):
        if not entry.get('exit_conditions'):
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
                    name = info.get('shortName', ticker)
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

### 📊 綜合趨勢評估
- **基本基調：** 偏多上攻 / 震盪洗盤 / 偏空回檔
- **五大起漲指標符合度：** X/5 項（列出符合與不符合的項目）

### 📈 明日實戰預測
- **預估漲跌幅區間：** 例如 +1.5% ~ +3.0%
- **多方攻擊關鍵壓力價：** [價位 + 技術依據]
- **空方防守關鍵支撐價：** [價位 + 技術依據]

### 🔍 各指標詳細分析
（逐項說明均線扣抵、量能、借券、KD、MACD、法人）

### ⚠️ 主要風險
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
        f'法人合計+{total_net:.0f}張，主力進場' if inst_ok else
        (f'法人合計{total_net:.0f}張，賣超' if total_net < 0 else '法人資料未取得'))

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

        # ── 借券賣出餘額（近 5 日趨勢）──────────────────────
        try:
            today_str = datetime.date.today().strftime('%Y%m%d')
            url = f'https://www.twse.com.tw/rwd/zh/shortselling/TWT93U?date={today_str}&response=json'
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            raw = json.loads(urllib.request.urlopen(req, timeout=10).read())
            short_data = {}
            if raw.get('stat') == 'OK':
                for row in raw.get('data', []):
                    if str(row[0]).strip() == code:
                        short_data = {
                            'lending_balance': safe_float(str(row[6]).replace(',','')),
                            'lending_sell':    safe_float(str(row[4]).replace(',','')),
                        }
                        break
            result['lending'] = short_data if short_data else None
        except Exception:
            result['lending'] = None

        # ── 當沖比 ────────────────────────────────────────────
        try:
            today_str = datetime.date.today().strftime('%Y%m%d')
            url = f'https://www.twse.com.tw/rwd/zh/dayTrading/TWTB4U?date={today_str}&response=json'
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            raw = json.loads(urllib.request.urlopen(req, timeout=10).read())
            day_trade_ratio = None
            if raw.get('stat') == 'OK':
                for row in raw.get('data', []):
                    if str(row[0]).strip() == code:
                        total_vol    = safe_float(str(row[2]).replace(',',''))
                        daytrade_vol = safe_float(str(row[4]).replace(',',''))
                        if total_vol > 0:
                            day_trade_ratio = round(daytrade_vol / total_vol * 100, 1)
                        break
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
    lines.append(f"- 乖離率（20MA）：{data.get('bias20', 'N/A')}%（{'✅ <6%，未過熱' if abs(data.get('bias20', 99)) < 6 else '⚠️ >6%，注意過熱'}）")

    # 五大指標
    lines.append(f"\n### 【五大起漲籌碼指標】")
    lines.append(f"1. 量能結構：{data.get('vol_structure', 'N/A')}（5MV={data.get('mv5',0):,} vs 20MV={data.get('mv20',0):,}）")

    lending = data.get('lending')
    if lending:
        lines.append(f"2. 借券賣出餘額：今日 {lending.get('lending_balance', 'N/A'):,} 張（借券賣出：{lending.get('lending_sell', 'N/A'):,}）")
    else:
        lines.append(f"2. 借券資料：今日尚未取得或非交易時間")

    rebound = data.get('rebound_pct', 0)
    lines.append(f"3. 乖離位階：乖離 {data.get('bias20', 'N/A')}%（{'✅ 符合' if 0 < data.get('bias20', 99) < 6 else '❌ 不符合'}）")
    lines.append(f"4. 反彈幅度：從近期低點 {data.get('recent_low', 'N/A')} 反彈 {rebound:.1f}%（{'✅ 符合 <15%' if rebound < 15 else '⚠️ 已超過15%'}）")

    dtr = data.get('day_trade_ratio')
    if dtr is not None:
        lines.append(f"5. 當沖比：{dtr}%（{'✅ <40%，籌碼穩定' if dtr < 40 else '⚠️ ≥40%，籌碼較虛浮'}）")
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
        lines.append(f"- 外資買賣超：{inst.get('foreign_net', 0):+,.0f} 張")
        lines.append(f"- 投信買賣超：{inst.get('trust_net', 0):+,.0f} 張")
        lines.append(f"- 自營商買賣超：{inst.get('dealer_net', 0):+,.0f} 張")
        lines.append(f"- 三大合計：{total:+,.0f} 張（{'主力買超' if total > 0 else '主力賣超'}）")
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
                send_msgs = [context_msg] + [m for m in messages if m.get('role') != 'user' or m != messages[-1]]
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


# ═══════════════════════════════════════════════════════════════════════════
#  AI Agent 自動化投資監控模組
# ═══════════════════════════════════════════════════════════════════════════

AGENT_FILE = os.path.join(os.path.dirname(__file__), 'agent_config.json')
_agent_lock = threading.Lock()

_AGENT_SYSTEM_PROMPT = """你是一位專業台股投資 AI 助理，負責分析持倉與市場機會，給出明確的操作建議。

你的職責：
1. 根據技術指標、籌碼數據、法人動向，對持倉股票給出【買進/加碼/持有/減碼/賣出】建議
2. 掃描低價但出現起漲訊號的股票，主動推薦值得關注的標的
3. 每次建議必須說明理由、關鍵支撐壓力價、預估漲跌幅

建議格式（每支股票）：
操作建議：買進/加碼/持有/減碼/賣出
原因：（50字以內，聚焦最關鍵的 2-3 個指標）
關鍵支撐：XX 元
關鍵壓力：XX 元
預估明日區間：+X% ~ +X%（或 -X% ~ -X%）
風險提示：（一句話）

判斷原則：
- 月線扣抵向上 + 乖離 < 6% + 量能健康 = 起漲條件
- 法人連買 + KD 低檔黃金交叉 = 強烈買訊
- 當沖比 > 50% + 爆量上影線 = 籌碼混亂，謹慎
- 跌破 MA20 且月線下彎 = 停損或減碼

回應使用繁體中文，語氣直接果斷，不要模糊建議，全程不要使用任何 emoji 或表情符號。"""


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
        'last_scan': '',
        'last_morning': '',
        'last_close': '',
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


def _agent_recommendation_text(data: dict, holding: dict = None) -> str:
    """Build context text for Claude agent recommendation."""
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
        cost   = round(buy_p * shares, 0)
        lines.append(f"持倉資訊：買入價 {buy_p} 元 / {shares} 股 / 未實現損益 {pnl:+.2f}%")

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
        lines.append(f"三大法人：外資 {inst.get('foreign_net',0):+,.0f} / 投信 {inst.get('trust_net',0):+,.0f} / 自營 {inst.get('dealer_net',0):+,.0f} / 合計 {total:+,.0f} 張")

    mg = data.get('margin')
    if mg:
        lines.append(f"融資餘額：{mg.get('margin_today',0):,.0f}（{mg.get('margin_chg',0):+,.0f}）  融券：{mg.get('short_today',0):,.0f}（{mg.get('short_chg',0):+,.0f}）")

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

    return '\n'.join(lines)


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
        return [s[0] for s in stocks[:limit]]
    except Exception as e:
        print(f'[Agent] universe error: {e}')
        return []


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
    """Analyze all holdings and return recommendation list."""
    holdings = cfg.get('holdings', [])
    if not holdings:
        return []

    output = []
    for h in holdings:
        code = h.get('code', '').strip().upper()
        if not code:
            continue
        data = _fetch_predict_data(code)
        ctx  = _agent_recommendation_text(data, h)
        news = _news_digest(data.get('name', code))
        if news:
            ctx += f"\n\n### 【近期新聞】\n{news}"
        try:
            resp = claude_client.messages.create(
                model='claude-opus-4-8',
                max_tokens=400,
                system=_AGENT_SYSTEM_PROMPT,
                messages=[{'role': 'user', 'content': f'請針對以下持倉股票給出明日操作建議（請一併考量消息面）：\n\n{ctx}'}],
            )
            rec_text = resp.content[0].text
        except Exception as e:
            rec_text = f'分析失敗：{e}'

        buy_p = h.get('buy_price', 0)
        price = data.get('price', 0)
        pnl   = round((price - buy_p) / buy_p * 100, 2) if buy_p else 0

        output.append({
            'code':       code,
            'name':       data.get('name', code),
            'price':      price,
            'buy_price':  buy_p,
            'pnl_pct':    pnl,
            'score':      _score_breakout(data),
            'recommendation': rec_text,
        })
    return output


def _run_agent_scan_with_ai(cfg: dict, claude_client) -> str:
    """Run breakout scan and ask Claude for top picks summary."""
    candidates = _run_agent_scan(cfg)
    if not candidates:
        return '本次掃描未找到符合條件的標的。'

    summary_lines = ['以下為本次掃描結果，請從中挑選最值得關注的 3-5 支，並說明理由：', '']
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
                holdings_analysis = _run_agent_holdings_analysis(cfg, client)
                scan_ai = _run_agent_scan_with_ai(cfg, client)

                lines = [f'[{today}] 早盤前分析報告', '']
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

                    lines = [f'[{now.strftime("%H:%M")}] 盤中監控']
                    urgent = [h for h in holdings_analysis if '賣出' in h['recommendation'] or '減碼' in h['recommendation']]
                    if urgent:
                        lines.append('!! 注意 !! 以下持倉出現賣出/減碼訊號：')
                        for h in urgent:
                            lines.append(f"{h['name']}（{h['code']}） 現價:{h['price']} 損益:{h['pnl_pct']:+.1f}%")
                            lines.append(h['recommendation'][:150])
                    else:
                        lines.append('持倉目前無急迫賣出訊號')
                        for h in holdings_analysis[:3]:
                            lines.append(f"{h['name']}（{h['code']}）{h['price']} → 得分{h['score']}/5")

                    cfg['last_scan'] = now.strftime('%Y-%m-%d %H:%M')
                    cfg['candidates'] = _run_agent_scan(cfg)
                    _agent_notify(cfg, '盤中監控', '\n'.join(lines))

            # Close summary (13:45-14:15)
            elif _is_close_time() and cfg.get('last_close', '') != today:
                cfg = _load_agent_cfg()
                holdings_analysis = _run_agent_holdings_analysis(cfg, client)
                candidates = _run_agent_scan(cfg)

                lines = [f'[{today}] 收盤分析總結', '']
                if holdings_analysis:
                    lines.append('--- 持倉收盤建議 ---')
                    for h in holdings_analysis:
                        lines.append(f"{h['name']} 現價:{h['price']} 損益:{h['pnl_pct']:+.1f}%")
                        lines.append(h['recommendation'][:200])
                        lines.append('')

                if candidates:
                    lines.append('--- 明日觀察名單 TOP5 ---')
                    for c in candidates[:5]:
                        lines.append(f"{c['name']}（{c['code']}） {c['price']}元 得分{c['score']}/5")

                cfg['last_close'] = today
                cfg['candidates'] = candidates
                _agent_notify(cfg, '收盤總結', '\n'.join(lines))

            # Daily strategy optimizer (after 15:00, once per day)
            elif now.hour >= 15 and cfg.get('last_optimize', '') != today:
                try:
                    _run_daily_optimizer(_load_agent_cfg(), client)
                except Exception as e:
                    print(f'[Agent] optimizer error: {e}')

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
        if safe.get('claude_api_key'):
            safe['claude_api_key'] = '***' + safe['claude_api_key'][-4:]
        return jsonify(safe)

    body = request.get_json(force=True)
    cfg  = _load_agent_cfg()

    for field in ['enabled', 'line_token', 'line_user_id',
                  'scan_price_min', 'scan_price_max', 'scan_min_score', 'scan_universe']:
        if field in body:
            cfg[field] = body[field]

    if body.get('claude_api_key') and not body['claude_api_key'].startswith('***'):
        cfg['claude_api_key'] = body['claude_api_key']

    _save_agent_cfg(cfg)
    return jsonify({'ok': True})


@app.route('/api/agent/holdings', methods=['GET', 'POST', 'DELETE'])
def agent_holdings_api():
    cfg = _load_agent_cfg()

    if request.method == 'GET':
        holdings = cfg.get('holdings', [])
        enriched = []
        for h in holdings:
            code  = h.get('code', '')
            price_data = _cache_get(f'agent_h_{code}')
            if not price_data:
                try:
                    stock = yf.Ticker(code + '.TW')
                    info  = stock.info
                    cur   = safe_float(info.get('currentPrice', info.get('regularMarketPrice', 0)))
                    if cur <= 0:
                        stock = yf.Ticker(code + '.TWO')
                        info  = stock.info
                        cur   = safe_float(info.get('currentPrice', info.get('regularMarketPrice', 0)))
                    price_data = {'price': cur, 'name': tw_cn_name(code, info.get('shortName', code))}
                    _cache_set(f'agent_h_{code}', price_data, ttl=120)
                except Exception:
                    price_data = {'price': 0, 'name': code}
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
        existing = [h for h in cfg.get('holdings', []) if h.get('code') != code]
        existing.append({
            'code':      code,
            'buy_price': safe_float(body.get('buy_price', 0)),
            'shares':    safe_int(body.get('shares', 0)),
            'date':      body.get('date', pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d')),
            'note':      body.get('note', ''),
        })
        cfg['holdings'] = existing
        _save_agent_cfg(cfg)
        return jsonify({'ok': True})

    if request.method == 'DELETE':
        code = request.args.get('code', '').strip().upper()
        cfg['holdings'] = [h for h in cfg.get('holdings', []) if h.get('code') != code]
        _save_agent_cfg(cfg)
        return jsonify({'ok': True})


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
                max_tokens=600,
                system=_AGENT_SYSTEM_PROMPT,
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
    body    = request.get_json(force=True)
    api_key = body.get('api_key', '') or _load_agent_cfg().get('claude_api_key', '')
    msgs    = body.get('messages', [])

    if not api_key:
        return jsonify({'error': '未設定 Claude API Key'}), 400

    def generate():
        try:
            client = anthropic.Anthropic(api_key=api_key)
            with client.messages.stream(
                model='claude-opus-4-8',
                max_tokens=1000,
                system=_AGENT_SYSTEM_PROMPT,
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
- 全程不要使用任何 emoji 或表情符號（包括 ⭐ ⚠️ 📌 等），改用文字標示重點
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

# 進場訊號（可由 AI 或使用者挑選）
ENTRY_SIGNALS = {
    'kd_gc':        'KD 黃金交叉',
    'ma_gc':        'MA5 上穿 MA20',
    'macd_gc':      'MACD 黃金交叉',
    'rsi_recover':  'RSI 由 30 以下回升',
    'breakout_ma20':'股價突破月線(MA20)',
    'buy_hold':     '買進並持有（純比較出場策略用）',
}


def _compute_entry_mask(hist, signal):
    """回傳 boolean numpy 陣列，標記每根 K 棒是否觸發進場訊號。"""
    close = hist['Close']
    high  = hist['High']
    low   = hist['Low']
    n     = len(close)

    if signal == 'buy_hold':
        mask = np.zeros(n, dtype=bool)
        if n: mask[0] = True
        return mask

    if signal == 'kd_gc':
        k_s, d_s = calc_kd(high, low, close)
        prev = (k_s.shift(1) <= d_s.shift(1))
        now  = (k_s > d_s)
        return (prev & now).fillna(False).values

    if signal == 'ma_gc':
        ma5  = close.rolling(5,  min_periods=1).mean()
        ma20 = close.rolling(20, min_periods=1).mean()
        prev = (ma5.shift(1) <= ma20.shift(1))
        now  = (ma5 > ma20)
        return (prev & now).fillna(False).values

    if signal == 'macd_gc':
        macd_s, sig_s, _ = calc_macd(close)
        prev = (macd_s.shift(1) <= sig_s.shift(1))
        now  = (macd_s > sig_s)
        return (prev & now).fillna(False).values

    if signal == 'rsi_recover':
        rsi  = calc_rsi(close)
        prev = (rsi.shift(1) <= 30)
        now  = (rsi > 30)
        return (prev & now).fillna(False).values

    if signal == 'breakout_ma20':
        ma20 = close.rolling(20, min_periods=1).mean()
        prev = (close.shift(1) <= ma20.shift(1))
        now  = (close > ma20)
        return (prev & now).fillna(False).values

    return np.zeros(n, dtype=bool)


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


def _run_backtest(ticker, signal='kd_gc', period='3y'):
    """對單一台股跑進場訊號 + 全出場策略網格回測，回傳排名結果。"""
    yf_ticker = tw_normalize(ticker)
    try:
        stock = yf.Ticker(yf_ticker)
        hist  = stock.history(period=period, interval='1d')
        if hist.empty or len(hist) < 60:
            yf_ticker = ticker.replace('.TW', '').replace('.TWO', '') + '.TWO'
            stock = yf.Ticker(yf_ticker)
            hist  = stock.history(period=period, interval='1d')
        if hist.empty or len(hist) < 60:
            return {'error': f'{ticker} 歷史資料不足，無法回測'}
    except Exception as e:
        return {'error': str(e)}

    if signal not in ENTRY_SIGNALS:
        signal = 'kd_gc'
    entry_mask = _compute_entry_mask(hist, signal)
    n_signals  = int(entry_mask.sum())

    code = ticker.replace('.TW', '').replace('.TWO', '')
    name = tw_cn_name(code, code)

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
        'signal':      signal,
        'signal_name': ENTRY_SIGNALS[signal],
        'period':      period,
        'bars':        len(hist),
        'n_signals':   n_signals,
        'buy_hold_ret':bh_ret,
        'best':        results[0] if results else None,
        'results':     results,
    }


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

    ranked.sort(key=lambda x: x['avg_backtest_ret'], reverse=True)
    summary = _optimizer_ai_summary(client, ranked)

    cfg = _load_agent_cfg()
    cfg['daily_strategy'] = {'date': today, 'ranked': ranked, 'summary': summary}
    cfg['last_optimize']  = today
    _save_agent_cfg(cfg)

    best = ranked[0] if ranked else None
    body_lines = [f'[{today}] 每日最佳策略優化', '']
    if best:
        picks = '、'.join(f"{p['name']}({p['code']})" for p in best['top_picks']) or '無'
        body_lines.append(f"今日最佳策略：{best['name']}（回測平均 {best['avg_backtest_ret']}%）")
        body_lines.append(f"候選：{picks}")
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


@app.route('/api/backtest/signals')
def backtest_signals():
    return jsonify(ENTRY_SIGNALS)


@app.route('/api/backtest/run', methods=['POST'])
def backtest_run():
    body   = request.get_json(force=True) or {}
    code   = str(body.get('code', '')).strip().upper().replace('.TW', '').replace('.TWO', '')
    signal = body.get('signal', 'kd_gc')
    period = body.get('period', '3y')
    if not code:
        return jsonify({'error': '請輸入股票代碼'}), 400
    result = _run_backtest(code, signal, period)
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════════════════
#  低位階永動機 — 春燕來了 低位階長期永動機投資法（台股）
#  以五年區間計算股價位階，鎖定相對歷史底部；判斷是否在年線之下、週 KD 是否低檔，
#  給出 撿壘 / 續抱 / 減碼 / 出場 訊號，並對監測標的做 -20% 硬停損提醒。
# ═══════════════════════════════════════════════════════════════════════════

def _perpetual_core(ticker, hist, entry_price=None, is_tw=True):
    """以五年日線資料計算位階與永動機訊號。hist 為 5y 日線 DataFrame。"""
    close = hist['Close'].dropna()
    n     = len(close)
    price = safe_float(close.iloc[-1])
    low5  = safe_float(hist['Low'].min())
    high5 = safe_float(hist['High'].max())
    rng   = high5 - low5

    # 位階 =（現價 − 五年低點）/（五年高點 − 五年低點），越低越接近底部
    position = round((price - low5) / rng * 100, 1) if rng > 0 else 50.0
    position = min(100.0, max(0.0, position))

    # 年線（MA240）
    ma240      = safe_float(close.rolling(min(240, n), min_periods=1).mean().iloc[-1])
    below_year = price < ma240
    year_bias  = round((price / ma240 - 1) * 100, 1) if ma240 > 0 else 0.0

    # 週 KD（由日線重採樣為週線，避免額外抓取）
    wk_k = wk_d = 50.0
    wk_low = wk_golden = False
    try:
        wh = hist.resample('W').agg({'High': 'max', 'Low': 'min', 'Close': 'last'}).dropna()
        if len(wh) >= 3:
            k, d = calc_kd(wh['High'], wh['Low'], wh['Close'])
            wk_k = round(safe_float(k.iloc[-1]), 1)
            wk_d = round(safe_float(d.iloc[-1]), 1)
            wk_low    = wk_k < 35
            wk_golden = (safe_float(k.iloc[-2]) <= safe_float(d.iloc[-2])) and (wk_k > wk_d)
    except Exception:
        pass

    # 從五年低點反彈幅度（仍處低檔初升段）
    bounce = round((price / low5 - 1) * 100, 1) if low5 > 0 else 0.0

    code = tw_display(ticker) if is_tw else ticker
    name = tw_cn_name(ticker, code) if is_tw else code

    # 停損狀態：-10% 警戒、-20% 硬停損
    pnl = round((price / entry_price - 1) * 100, 1) if entry_price else None
    stop_status = 'ok'
    if entry_price:
        if   pnl <= -20: stop_status = 'hard'
        elif pnl <= -10: stop_status = 'warn'

    reasons = []
    if stop_status == 'hard':
        signal, sclass = '出場', 'exit'
        reasons.append(f'跌破進場價 {pnl}%，觸發 -20% 硬停損，建議出場')
    else:
        if position < 35 and below_year and (wk_low or wk_golden):
            signal, sclass = '撿壘', 'buy'
            reasons.append(f'位階 {position}（五年區間相對低檔），股價在年線之下')
            if wk_golden:
                reasons.append(f'週 KD 低檔金叉（K={wk_k} / D={wk_d}），起漲訊號浮現')
            elif wk_low:
                reasons.append(f'週 KD 低檔（K={wk_k}），打底蓄勢')
        elif position < 35:
            signal, sclass = '觀望', 'wait'
            reasons.append(f'位階低（{position}）但尚無起漲訊號（年線之上或週 KD 未低檔），續打底')
        elif position < 58:
            signal, sclass = '續抱', 'hold'
            reasons.append(f'位階 {position}，初升段，趨勢轉強可續抱')
        elif position < 80:
            signal, sclass = '減碼', 'reduce'
            reasons.append(f'位階偏高（{position}），逢高分批減碼鎖利')
        else:
            signal, sclass = '出場', 'exit'
            reasons.append(f'位階過高（{position}），五年相對高檔，建議出場')
        if stop_status == 'warn':
            reasons.append(f'目前 {pnl}%，已逾 -10% 警戒區，留意 -20% 硬停損')

    return {
        'ticker': ticker, 'code': code, 'name': name, 'isTw': is_tw,
        'price': round(price, 2),
        'low5': round(low5, 2), 'high5': round(high5, 2),
        'position': position,
        'ma240': round(ma240, 2), 'belowYear': below_year, 'yearBias': year_bias,
        'wkK': wk_k, 'wkD': wk_d, 'wkLow': wk_low, 'wkGolden': wk_golden,
        'bounce': bounce,
        'signal': signal, 'signalClass': sclass,
        'reasons': reasons,
        'entryPrice': round(entry_price, 2) if entry_price else None,
        'pnlPct': pnl,
        'stopStatus': stop_status,
        'stopHardPrice': round(entry_price * 0.8, 2) if entry_price else None,
    }


def _perpetual_fetch(ticker, entry_price=None, is_tw=True):
    """抓取 5 年日線並回傳位階分析；失敗回 None。"""
    try:
        stock = yf.Ticker(ticker)
        hist  = stock.history(period='5y')
        if hist.empty or len(hist) < 60:
            return None
        if safe_float(hist['Close'].dropna().iloc[-1]) <= 0:
            return None
        return _perpetual_core(ticker, hist, entry_price, is_tw)
    except Exception:
        return None


@app.route('/perpetual')
def perpetual_page():
    from flask import make_response
    resp = make_response(render_template('perpetual.html'))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    return resp


@app.route('/api/perpetual/universe')
def perpetual_universe():
    return jsonify({'tw': TW_SCREENER_UNIVERSE, 'us': US_SCREENER_UNIVERSE})


@app.route('/api/perpetual/analyze', methods=['POST'])
def perpetual_analyze():
    data  = request.get_json(force=True) or {}
    raw   = str(data.get('code', '')).strip()
    is_tw = bool(data.get('isTw', True))
    entry = safe_float(data.get('entry', 0)) or None
    if not raw:
        return jsonify({'error': '請輸入股票代碼'}), 400
    ticker = tw_normalize(raw) if is_tw else raw.upper()
    res = _perpetual_fetch(ticker, entry, is_tw)
    if not res:
        return jsonify({'error': '查無資料或上市未滿一定期間，請改用其他代碼'}), 404
    return jsonify(res)


@app.route('/api/perpetual/scan', methods=['POST'])
def perpetual_scan():
    data    = request.get_json(force=True) or {}
    is_tw   = bool(data.get('isTw', True))
    sectors = data.get('sectors', [])
    universe = TW_SCREENER_UNIVERSE if is_tw else US_SCREENER_UNIVERSE

    codes = []
    if sectors:
        for s in sectors:
            codes.extend(universe.get(s, []))
    else:
        for lst in universe.values():
            codes.extend(lst)
    codes = list(dict.fromkeys(codes))  # 去重保序
    if not codes:
        return jsonify({'error': '請選擇要掃描的族群'}), 400
    if len(codes) > 150:
        codes = codes[:150]

    tickers = [tw_normalize(c) if is_tw else c.upper() for c in codes]
    results = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(_perpetual_fetch, t, None, is_tw): t for t in tickers}
        for f in as_completed(futs):
            try:
                r = f.result()
                # 只保留低位階（<35）且有起漲跡象的標的
                if r and r['position'] < 35 and (r['belowYear'] and (r['wkLow'] or r['wkGolden'])):
                    results.append(r)
            except Exception:
                pass

    results.sort(key=lambda x: x['position'])
    return jsonify({'results': results, 'total': len(tickers), 'matched': len(results)})


# ── 全上市櫃掃描（背景執行 + 進度輪詢，結果寫檔供多 worker 共享）─────────────
PP_SCAN_FILE = os.path.join(os.path.dirname(__file__), 'perpetual_scan.json')
_pp_scan_lock = threading.Lock()


def _pp_is_stock_or_etf(code: str) -> bool:
    """全上市櫃普通股 + ETF；排除權證（6 碼）、特別股（含字母）。"""
    if not code.isdigit():
        return False
    if len(code) == 4 and code[0] != '0':      # 普通股 1101~9962、TDR 91xx
        return True
    if code.startswith('00') and 4 <= len(code) <= 6:  # ETF 0050 / 00878 …
        return True
    return False


def _pp_full_codes() -> list:
    with _tw_name_lock:
        codes = [c for c in _tw_name_cache.keys() if _pp_is_stock_or_etf(c)]
    return sorted(set(codes))


def _pp_scan_load() -> dict:
    try:
        if os.path.exists(PP_SCAN_FILE):
            with open(PP_SCAN_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        pass
    return {'running': False, 'total': 0, 'done': 0, 'matched': 0,
            'results': [], 'started': '', 'finished': ''}


def _pp_scan_save(st: dict):
    with open(PP_SCAN_FILE, 'w', encoding='utf-8') as f:
        json.dump(st, f, ensure_ascii=False)


def _run_full_scan():
    """背景：批次下載全上市櫃 5 年資料，篩出低位階起漲標的，邊跑邊更新進度檔。"""
    codes   = _pp_full_codes()
    tickers = [tw_normalize(c) for c in codes]
    now_str = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M')
    with _pp_scan_lock:
        _pp_scan_save({'running': True, 'total': len(tickers), 'done': 0,
                       'matched': 0, 'results': [], 'started': now_str, 'finished': ''})

    results = []
    CHUNK = 60
    for i in range(0, len(tickers), CHUNK):
        batch = tickers[i:i + CHUNK]
        try:
            data = yf.download(batch, period='5y', group_by='ticker',
                               threads=True, progress=False, auto_adjust=True)
        except Exception:
            data = None
        for t in batch:
            try:
                if data is None:
                    continue
                hist = data[t] if (len(batch) > 1 and t in data.columns.get_level_values(0)) else data
                hist = hist.dropna(how='all')
                if hist.empty or len(hist) < 60:
                    continue
                r = _perpetual_core(t, hist, None, True)
                if r['position'] < 35 and r['belowYear'] and (r['wkLow'] or r['wkGolden']):
                    results.append(r)
            except Exception:
                pass
        ranked = sorted(results, key=lambda x: x['position'])
        with _pp_scan_lock:
            st = _pp_scan_load()
            st.update(done=min(i + CHUNK, len(tickers)), matched=len(ranked), results=ranked)
            _pp_scan_save(st)

    fin = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M')
    with _pp_scan_lock:
        st = _pp_scan_load()
        st.update(running=False, done=len(tickers), matched=len(results),
                  results=sorted(results, key=lambda x: x['position']), finished=fin)
        _pp_scan_save(st)


@app.route('/api/perpetual/scan_all', methods=['POST'])
def perpetual_scan_all():
    with _pp_scan_lock:
        st = _pp_scan_load()
        if st.get('running') and st.get('started'):
            try:
                age = (pd.Timestamp.now(tz='Asia/Taipei') -
                       pd.Timestamp(st['started'], tz='Asia/Taipei')).total_seconds()
            except Exception:
                age = 9999
            if age < 1200:   # 既有任務未逾 20 分鐘，沿用不重啟
                return jsonify({'ok': True, 'already': True, 'state': st})
        if not _pp_full_codes():
            return jsonify({'error': '全上市櫃清單尚未就緒（名稱快取載入中），請稍候再試'}), 503
        _pp_scan_save({'running': True, 'total': len(_pp_full_codes()), 'done': 0,
                       'matched': 0, 'results': [],
                       'started': pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M'),
                       'finished': ''})
    threading.Thread(target=_run_full_scan, daemon=True).start()
    return jsonify({'ok': True, 'started': True})


@app.route('/api/perpetual/scan_all/status')
def perpetual_scan_all_status():
    with _pp_scan_lock:
        return jsonify(_pp_scan_load())


def _perpetual_line_text(res, title):
    arrow = {'buy': '撿壘', 'hold': '續抱', 'reduce': '減碼', 'exit': '出場', 'wait': '觀望'}
    lines = [f"【低位階永動機】{res['name']}（{res['code']}）", title,
             f"現價 {res['price']}・位階 {res['position']}（五年 {res['low5']}~{res['high5']}）",
             f"訊號：{res['signal']}"]
    if res.get('pnlPct') is not None:
        lines.append(f"進場 {res['entryPrice']}・損益 {res['pnlPct']}%（硬停損價 {res['stopHardPrice']}）")
    if res.get('reasons'):
        lines.append('・'.join(res['reasons'][:2]))
    return '\n'.join(lines)


@app.route('/api/perpetual/monitor/list')
def perpetual_monitor_list():
    with _monitor_lock:
        cfg = _load_monitor_cfg()
    watches = cfg.get('perpetual', {})
    out = []
    for ticker, w in watches.items():
        out.append({
            'ticker': ticker, 'code': tw_display(ticker) if w.get('is_tw', True) else ticker,
            'name': w.get('name', ''), 'isTw': w.get('is_tw', True),
            'entryPrice': w.get('entry_price'),
            'registeredAt': w.get('registered_at', ''),
            'lastSignal': w.get('last_signal', ''),
            'lastCheck': w.get('last_check', ''),
            'lastResult': w.get('last_result'),
        })
    out.sort(key=lambda x: x.get('registeredAt', ''), reverse=True)
    return jsonify({'watches': out})


@app.route('/api/perpetual/monitor/add', methods=['POST'])
def perpetual_monitor_add():
    data  = request.get_json(force=True) or {}
    raw   = str(data.get('code', '')).strip()
    is_tw = bool(data.get('isTw', True))
    entry = safe_float(data.get('entry', 0)) or None
    line_token   = data.get('line_token', '')
    line_user_id = data.get('line_user_id', '')
    if not raw:
        return jsonify({'error': '請輸入股票代碼'}), 400
    ticker = tw_normalize(raw) if is_tw else raw.upper()

    res = _perpetual_fetch(ticker, entry, is_tw)
    if not res:
        return jsonify({'error': '查無資料，無法加入監測'}), 404

    now_str = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M')
    with _monitor_lock:
        cfg = _load_monitor_cfg()
        cfg.setdefault('perpetual', {})
        existing = cfg['perpetual'].get(ticker, {})
        cfg['perpetual'][ticker] = {
            'is_tw': is_tw,
            'entry_price': entry,
            'name': res['name'],
            'line_token':   line_token or existing.get('line_token', cfg.get('line_token', '')),
            'line_user_id': line_user_id or existing.get('line_user_id', cfg.get('line_user_id', '')),
            'registered_at': existing.get('registered_at', now_str),
            'last_signal': res['signal'],
            'last_check': now_str,
            'last_result': res,
            'last_notify_time': existing.get('last_notify_time', ''),
        }
        _save_monitor_cfg(cfg)
    return jsonify({'ok': True, 'ticker': ticker, 'result': res})


@app.route('/api/perpetual/monitor/remove', methods=['POST'])
def perpetual_monitor_remove():
    data   = request.get_json(force=True) or {}
    ticker = str(data.get('ticker', '')).strip()
    if not ticker:
        return jsonify({'error': 'ticker required'}), 400
    with _monitor_lock:
        cfg = _load_monitor_cfg()
        cfg.get('perpetual', {}).pop(ticker, None)
        _save_monitor_cfg(cfg)
    return jsonify({'ok': True})


def _run_perpetual_watch():
    """背景：對監測中的永動機標的更新訊號，訊號轉強/減碼/出場或觸發硬停損時推播 LINE。"""
    with _monitor_lock:
        cfg = _load_monitor_cfg()
    watches = cfg.get('perpetual', {})
    if not watches:
        return
    now    = pd.Timestamp.now(tz='Asia/Taipei')
    now_str = now.strftime('%Y-%m-%d %H:%M')
    for ticker, w in list(watches.items()):
        try:
            res = _perpetual_fetch(ticker, w.get('entry_price'), w.get('is_tw', True))
            if not res:
                continue
            sig       = res['signal']
            last_sig  = w.get('last_signal')
            notify, title = False, ''
            if res['stopStatus'] == 'hard':
                notify, title = True, '觸發 -20% 硬停損'
            elif sig in ('撿壘', '減碼', '出場') and sig != last_sig:
                notify, title = True, f'訊號轉為「{sig}」'

            last_notify = w.get('last_notify_time', '')
            cooldown_ok = (not last_notify or
                (now - pd.Timestamp(last_notify, tz='Asia/Taipei')).total_seconds() > 14400)
            line_token   = w.get('line_token', '')
            line_user_id = w.get('line_user_id', '')

            with _monitor_lock:
                cfg2 = _load_monitor_cfg()
                if ticker not in cfg2.get('perpetual', {}):
                    continue
                cfg2['perpetual'][ticker]['last_signal'] = sig
                cfg2['perpetual'][ticker]['last_check']  = now_str
                cfg2['perpetual'][ticker]['last_result'] = res
                if notify and cooldown_ok and line_token and line_user_id:
                    cfg2['perpetual'][ticker]['last_notify_time'] = now_str
                    _save_monitor_cfg(cfg2)
                    _push_line_msg(line_token, line_user_id, _perpetual_line_text(res, title))
                else:
                    _save_monitor_cfg(cfg2)
        except Exception as e:
            print(f'[Perpetual] {ticker}: {e}')


# 掛到既有背景掃描鏈（在出場警示之後再跑永動機監測）
_prev_scan_fn = sys.modules[__name__].__dict__['_run_server_scan']

def _run_server_scan_with_perp():
    _prev_scan_fn()
    try:
        _run_perpetual_watch()
    except Exception as e:
        print(f'[Perpetual] watch loop error: {e}')

sys.modules[__name__].__dict__['_run_server_scan'] = _run_server_scan_with_perp


# ═══════════════════════════════════════════════════════════════════════════
#  績優股清單 — 財報狗選股法（被低估的績優股）
#  先剔除自由現金流報酬率三年衰退的公司；再以「三年平均自由現金流報酬率、本益比、
#  股價淨值比、殖利率」四因子各自排名加總，綜合分數越小＝越被低估，排越前面。
#  單檔財報資料快取 6 小時（第二次跑分更快）。預設台股，可切美股。
# ═══════════════════════════════════════════════════════════════════════════

BLUECHIP_CACHE_FILE = os.path.join(os.path.dirname(__file__), 'bluechip_cache.json')
BLUECHIP_SCAN_FILE  = os.path.join(os.path.dirname(__file__), 'bluechip_scan.json')
BLUECHIP_TTL = 6 * 3600
_bluechip_lock = threading.Lock()


def _bc_json_load(path, default):
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        pass
    return default


def _bc_json_save(path, obj):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False)


def _bluechip_compute(ticker, is_tw):
    """抓單檔財報因子：三年平均自由現金流報酬率、本益比、股價淨值比、殖利率。"""
    try:
        stock = yf.Ticker(ticker)
        info  = stock.info
        mktcap = safe_float(info.get('marketCap', 0))
        price  = safe_float(info.get('currentPrice', info.get('regularMarketPrice', 0)))
        pe     = safe_float(info.get('trailingPE', 0))
        pb     = safe_float(info.get('priceToBook', 0))
        dy     = safe_float(info.get('dividendYield', 0))   # 既有慣例：視為百分比
        if mktcap <= 0 or price <= 0:
            return {'ok': False}

        # 自由現金流序列（年報，最多 3 年；欄位由新到舊）
        fcf_vals = []
        try:
            cf = stock.cashflow
            if cf is not None and not cf.empty:
                fcf_row = None
                for lbl in ['Free Cash Flow']:
                    if lbl in cf.index:
                        fcf_row = cf.loc[lbl]; break
                if fcf_row is None:
                    ocf = capex = None
                    for lbl in ['Operating Cash Flow', 'Total Cash From Operating Activities']:
                        if lbl in cf.index: ocf = cf.loc[lbl]; break
                    for lbl in ['Capital Expenditure', 'Capital Expenditures']:
                        if lbl in cf.index: capex = cf.loc[lbl]; break
                    if ocf is not None and capex is not None:
                        fcf_row = ocf + capex
                if fcf_row is not None:
                    for x in fcf_row.values[:3]:
                        fx = safe_float(x)
                        if x == x:           # 非 NaN
                            fcf_vals.append(fx)
        except Exception:
            pass

        if len(fcf_vals) < 2:
            return {'ok': False}

        avg_fcf     = sum(fcf_vals) / len(fcf_vals)
        avg3_yield  = round(avg_fcf / mktcap * 100, 2)
        declining   = fcf_vals[0] < fcf_vals[-1]   # 最新年 < 最舊年 → 視為衰退
        name = tw_cn_name(ticker, tw_display(ticker)) if is_tw \
            else (info.get('shortName') or info.get('longName') or ticker)[:30]

        return {
            'ok': True, 'name': name, 'price': round(price, 2),
            'avg3FcfYield': avg3_yield,
            'fcfYields': [round(v / 1e9, 2) for v in fcf_vals],
            'declining': declining,
            'pe': round(pe, 2), 'pb': round(pb, 2), 'divYield': round(dy, 2),
        }
    except Exception:
        return {'ok': False}


def _run_bluechip_scan(market):
    is_tw    = (market != 'us')
    universe = TW_SCREENER_UNIVERSE if is_tw else US_SCREENER_UNIVERSE
    codes    = []
    for lst in universe.values():
        codes.extend(lst)
    codes   = list(dict.fromkeys(codes))
    tickers = [tw_normalize(c) if is_tw else c.upper() for c in codes]

    now     = time.time()
    now_str = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M')
    cache   = _bc_json_load(BLUECHIP_CACHE_FILE, {})

    raw, to_fetch = {}, []
    for t in tickers:
        e = cache.get(t)
        if e and (now - e.get('ts', 0) < BLUECHIP_TTL):
            raw[t] = e['data']
        else:
            to_fetch.append(t)

    def _set_state(done, running=True, results=None, finished=''):
        with _bluechip_lock:
            st = {'running': running, 'market': market, 'total': len(tickers),
                  'done': done, 'matched': len(results or []),
                  'results': results or [], 'started': now_str, 'finished': finished}
            _bc_json_save(BLUECHIP_SCAN_FILE, st)

    _set_state(len(raw))

    done = len(raw)
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(_bluechip_compute, t, is_tw): t for t in to_fetch}
        for f in as_completed(futs):
            t = futs[f]
            try:
                raw[t] = f.result() or {'ok': False}
            except Exception:
                raw[t] = {'ok': False}
            cache[t] = {'ts': now, 'data': raw[t]}
            done += 1
            if done % 20 == 0:
                _set_state(done)

    with _bluechip_lock:
        _bc_json_save(BLUECHIP_CACHE_FILE, cache)

    # 剔除：資料不全 / FCF 三年衰退 / 平均 FCF 報酬率為負 / 無正本益比、淨值比
    survivors = [t for t, d in raw.items()
                 if d.get('ok') and not d['declining']
                 and d['avg3FcfYield'] > 0 and d['pe'] > 0 and d['pb'] > 0]

    def _rank(key, higher_better):
        order = sorted(survivors, key=lambda t: raw[t][key], reverse=higher_better)
        return {t: i + 1 for i, t in enumerate(order)}

    r_fcf = _rank('avg3FcfYield', True)
    r_pe  = _rank('pe', False)
    r_pb  = _rank('pb', False)
    r_dy  = _rank('divYield', True)

    results = []
    for t in survivors:
        d = raw[t]
        score = r_fcf[t] + r_pe[t] + r_pb[t] + r_dy[t]
        results.append({
            'ticker': t, 'code': tw_display(t) if is_tw else t,
            'name': d['name'], 'price': d['price'],
            'score': score,
            'avg3FcfYield': d['avg3FcfYield'], 'pe': d['pe'], 'pb': d['pb'], 'divYield': d['divYield'],
            'rankFcf': r_fcf[t], 'rankPe': r_pe[t], 'rankPb': r_pb[t], 'rankDy': r_dy[t],
            'declining': d['declining'],
        })
    results.sort(key=lambda x: (x['score'], x['pe']))

    fin = pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M')
    _set_state(len(tickers), running=False, results=results, finished=fin)


@app.route('/bluechip')
def bluechip_page():
    from flask import make_response
    resp = make_response(render_template('bluechip.html'))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    return resp


@app.route('/api/bluechip/scan', methods=['POST'])
def bluechip_scan():
    data   = request.get_json(force=True) or {}
    market = 'us' if str(data.get('market', 'tw')).lower() == 'us' else 'tw'
    with _bluechip_lock:
        st = _bc_json_load(BLUECHIP_SCAN_FILE, {})
        if st.get('running') and st.get('started'):
            try:
                age = (pd.Timestamp.now(tz='Asia/Taipei') -
                       pd.Timestamp(st['started'], tz='Asia/Taipei')).total_seconds()
            except Exception:
                age = 9999
            if age < 1200:
                return jsonify({'ok': True, 'already': True, 'state': st})
        _bc_json_save(BLUECHIP_SCAN_FILE,
                      {'running': True, 'market': market, 'total': 0, 'done': 0,
                       'matched': 0, 'results': [],
                       'started': pd.Timestamp.now(tz='Asia/Taipei').strftime('%Y-%m-%d %H:%M'),
                       'finished': ''})
    threading.Thread(target=_run_bluechip_scan, args=(market,), daemon=True).start()
    return jsonify({'ok': True, 'started': True})


@app.route('/api/bluechip/scan/status')
def bluechip_scan_status():
    with _bluechip_lock:
        return jsonify(_bc_json_load(BLUECHIP_SCAN_FILE,
                       {'running': False, 'total': 0, 'done': 0, 'matched': 0,
                        'results': [], 'started': '', 'finished': '', 'market': 'tw'}))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6001, debug=False)

from flask import Flask, render_template, jsonify, request
import yfinance as yf
import pandas as pd
import numpy as np
import warnings
import time
import requests as _requests
from xml.etree import ElementTree as ET
import urllib.parse
warnings.filterwarnings('ignore')

app = Flask(__name__)

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
    if price >= week52h * 0.97:
        cats.append({'num': 1, 'text': '突破或接近52週高點，歷史強勢突破信號',
                     'sub': '價格創新高，市場認可度顯著提升'})
    if macd > dea and macd > 0:
        cats.append({'num': len(cats)+1, 'text': 'MACD 技術面轉強，動能向上',
                     'sub': '金叉在零軸上方，短中期均偏多'})
    if vol_ratio >= 1.5:
        cats.append({'num': len(cats)+1, 'text': f'成交量異常放大（{vol_ratio:.1f}x 均量）',
                     'sub': '機構資金積極布局跡象明顯'})
    if price > ma5 > ma20 > ma60:
        cats.append({'num': len(cats)+1, 'text': '均線多頭排列完整，趨勢強勢',
                     'sub': '短中長期均線支撐，回撤布局機會'})
    target = safe_float(info.get('targetMeanPrice', 0))
    if target > price * 1.1:
        cats.append({'num': len(cats)+1, 'text': f'分析師目標價 ${target:.2f}，具上漲空間',
                     'sub': f'較現價有 {(target/price-1)*100:.0f}% 潛在漲幅'})
    extras = [
        {'text': '財報週期臨近，業績催化劑持續',       'sub': '關注收入成長與利潤率改善趨勢'},
        {'text': '產業趨勢受益，長期成長邏輯明確',     'sub': '市場份額擴大，商業模式持續優化'},
        {'text': '技術面關鍵位置蓄積，突破動能累積',   'sub': '觀察成交量配合情況確認方向'},
        {'text': '機構持股比例提升，籌碼結構改善',     'sub': '長線資金介入增強價格支撐'},
    ]
    while len(cats) < 4:
        cats.append({'num': len(cats)+1, **extras[len(cats) % len(extras)]})
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

def gen_risks(price, ma20, rsi, vol_ratio, week52h, pe=0, fwd_pe=0, beta=1.0, debt_equity=0):
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

    # Always-present macro & business risks
    risks.append({'level':'medium', 'category':'總經風險', 'text':'聯準會利率政策與通膨數據仍具不確定性，高估值成長股對利率敏感度高'})
    risks.append({'level':'low',    'category':'業務風險', 'text':'市場競爭加劇與技術迭代加速，財報不如預期或展望保守將引發短期大幅波動'})
    risks.append({'level':'low',    'category':'地緣風險', 'text':'中美貿易摩擦、地緣政治緊張局勢可能影響供應鏈與市場情緒'})

    return risks[:6]

def gen_strategy(price, ma5, ma20, ma60, rsi, levels):
    stop = max(levels['support1'] * 0.97, price * 0.90)
    if price > ma20 and rsi < 70:
        long_t  = f'逢回布局，回測 MA20（${ma20:.2f}）附近加倉，止損設 MA60（${ma60:.2f}）下方 3%'
        swing_t = f'波段操作：突破近期高點 ${levels["resistance1"]:.2f} 後加碼，回踩 MA20 止損'
        short_t = f'短線留意支撐位 ${levels["support1"]:.2f} 附近反彈機會，嚴格設止損'
    else:
        long_t  = f'等待股價站穩 MA60（${ma60:.2f}）後再布局，降低進場風險'
        swing_t = f'等待回測 MA20（${ma20:.2f}）確認支撐後入場，止損設前低'
        short_t = f'技術面偏弱，觀望為主，等待均線金叉信號再行動'
    return {
        'long': long_t, 'swing': swing_t, 'short': short_t,
        'stopLoss':       round(stop, 2),
        'bullTarget':     round(price * 1.30, 1),
        'neutralTarget':  round(price * 1.12, 1),
        'bearTarget':     round(price * 0.85, 1),
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
        price = safe_float(hist['Close'].iloc[-1])
        prev  = safe_float(hist['Close'].iloc[-2])
        change     = price - prev
        change_pct = change / prev * 100 if prev else 0

        ma5    = safe_float(hist['MA5'].iloc[-1])
        ma20   = safe_float(hist['MA20'].iloc[-1])
        ma60   = safe_float(hist['MA60'].iloc[-1])
        macd_v = safe_float(hist['MACD'].iloc[-1])
        dea_v  = safe_float(hist['Signal'].iloc[-1])
        macd_h = safe_float(hist['MACDHist'].iloc[-1])
        rsi_v  = safe_float(hist['RSI'].iloc[-1])

        avg_vol   = safe_float(hist['Volume'].rolling(20).mean().iloc[-1])
        curr_vol  = safe_float(hist['Volume'].iloc[-1])
        vol_ratio = curr_vol / avg_vol if avg_vol > 0 else 1.0

        week52h = safe_float(info.get('fiftyTwoWeekHigh', hist['High'].max()))
        week52l = safe_float(info.get('fiftyTwoWeekLow',  hist['Low'].min()))

        bb_u = safe_float(hist['BB_upper'].iloc[-1])
        bb_m = safe_float(hist['BB_mid'].iloc[-1])
        bb_l = safe_float(hist['BB_lower'].iloc[-1])
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
                                debt_equity=safe_float(info.get('debtToEquity',0)))
        strategy    = gen_strategy(price, ma5, ma20, ma60, rsi_v, levels)
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
            'open':         round(safe_float(hist['Open'].iloc[-1]), 2),
            'high':         round(safe_float(hist['High'].iloc[-1]), 2),
            'low':          round(safe_float(hist['Low'].iloc[-1]), 2),
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
            'divYield':     round(safe_float(info.get('dividendYield', 0)) * 100, 2),
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
                 pe=0, fwd_pe=0, beta=1.0, debt_equity=0, is_etf=False):
    risks = []
    from_high = (price - week52h) / week52h * 100 if week52h > 0 else 0
    ref_pe = fwd_pe if fwd_pe > 0 else pe

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

    if is_etf:
        risks.append({'level':'low', 'category':'追蹤風險', 'text':'ETF 追蹤誤差與折溢價可能影響實際報酬，建議定期確認 NAV 與市價差異'})

    risks.append({'level':'medium', 'category':'地緣風險', 'text':'兩岸關係緊張及地緣政治局勢仍是台股最大不確定因素，可能引發外資快速撤離並衝擊市場'})
    risks.append({'level':'medium', 'category':'總經風險', 'text':'台灣央行利率政策、新台幣匯率走勢及全球景氣循環均對台股形成壓力，需密切追蹤'})
    risks.append({'level':'low',    'category':'外資風險', 'text':'外資持股比例高，全球風險趨避情緒升溫時可能引發大量賣超，衝擊市場流動性'})
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
        extras = [
            {'text': '長期定期定額最佳工具，分散風險效果佳', 'sub': '追蹤指數，分散個股風險，適合長期穩健投資人'},
            {'text': '費用率低廉，長期複利效果顯著優越',     'sub': '相較主動基金費用低，長期績效差異大'},
            {'text': '配息穩定，適合退休規劃與現金流需求',   'sub': '定期配息提供穩定現金流，適合保守型投資人'},
            {'text': '流動性佳，買賣彈性高於一般基金',       'sub': '交易所掛牌，隨時買賣，不受申購贖回限制'},
        ]
    else:
        extras = [
            {'text': 'AI 供應鏈受惠，台灣半導體優勢持續',   'sub': '全球 AI 基礎建設需求旺盛，台廠訂單能見度高'},
            {'text': '三大法人合計買超，籌碼結構穩定',       'sub': '外資＋投信＋自營商共同護盤，主力資金積極介入'},
            {'text': '配息穩定，殖利率具吸引力',             'sub': '高殖利率在利率環境中具防禦優勢，吸引存股族'},
            {'text': '技術面關鍵位置蓄積，突破動能累積中',   'sub': '等待成交量配合確認突破，上漲空間可期'},
        ]
    while len(cats) < 4:
        cats.append({'num': len(cats)+1, **extras[len(cats) % len(extras)]})
    return cats[:4]


# ── Taiwan Routes ─────────────────────────────────────────────────────
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

        price = safe_float(hist['Close'].iloc[-1])
        prev  = safe_float(hist['Close'].iloc[-2])
        change     = price - prev
        change_pct = change / prev * 100 if prev else 0

        ma5    = safe_float(hist['MA5'].iloc[-1])
        ma20   = safe_float(hist['MA20'].iloc[-1])
        ma60   = safe_float(hist['MA60'].iloc[-1])
        macd_v = safe_float(hist['MACD'].iloc[-1])
        dea_v  = safe_float(hist['Signal'].iloc[-1])
        macd_h = safe_float(hist['MACDHist'].iloc[-1])
        rsi_v  = safe_float(hist['RSI'].iloc[-1])

        avg_vol  = safe_float(hist['Volume'].rolling(20).mean().iloc[-1])
        curr_vol = safe_float(hist['Volume'].iloc[-1])
        vol_ratio = curr_vol / avg_vol if avg_vol > 0 else 1.0

        week52h = safe_float(info.get('fiftyTwoWeekHigh', hist['High'].max()))
        week52l = safe_float(info.get('fiftyTwoWeekLow',  hist['Low'].min()))

        bbu = safe_float(hist['BB_upper'].iloc[-1])
        bbm = safe_float(hist['BB_mid'].iloc[-1])
        bbl = safe_float(hist['BB_lower'].iloc[-1])
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
                                   is_etf=is_etf)
        strategy    = gen_strategy(price, ma5, ma20, ma60, rsi_v, levels)
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
            etf_data = {
                'totalAssets':   round(ta / 1e8, 1),
                'expenseRatio':  round(er * 100, 4) if er > 0 else 0,
                'threeYrReturn': round(safe_float(info.get('threeYearAverageReturn', 0)) * 100, 2),
                'fiveYrReturn':  round(safe_float(info.get('fiveYearAverageReturn',  0)) * 100, 2),
                'ytdReturn':     round(safe_float(info.get('ytdReturn', 0)) * 100, 2),
                'category':      info.get('category', ''),
                'fundFamily':    info.get('fundFamily', ''),
            }

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
            'open':          round(safe_float(hist['Open'].iloc[-1]), 2),
            'high':          round(safe_float(hist['High'].iloc[-1]), 2),
            'low':           round(safe_float(hist['Low'].iloc[-1]), 2),
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
            'divYield':      round(safe_float(info.get('dividendYield', 0)) * 100, 2),
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
            name = info.get('shortName', '') or info.get('longName', '') or ''
            query = f'{code} {name} 台股' if name else f'{code} 台股'
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5999, debug=False)

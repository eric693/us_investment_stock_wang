from flask import Flask, render_template, jsonify, request
import yfinance as yf
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

app = Flask(__name__)

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

def gen_risks(price, ma20, rsi, vol_ratio, week52h):
    risks = []
    from_high = (price - week52h) / week52h * 100 if week52h > 0 else 0
    if rsi > 70:
        risks.append({'level': 'high',   'text': f'技術面超買（RSI {rsi:.0f}），短期回調壓力大'})
    if from_high > -5:
        risks.append({'level': 'medium', 'text': f'股價近52週高點（距頂 {from_high:.1f}%），壓力較大'})
    if price < ma20:
        risks.append({'level': 'high',   'text': '股價跌破 MA20 支撐，趨勢可能轉弱'})
    if vol_ratio > 3:
        risks.append({'level': 'medium', 'text': '成交量極度放大，可能出現獲利了結賣壓'})
    risks.append({'level': 'medium', 'text': '宏觀環境變化（利率、地緣政治）影響市場情緒'})
    risks.append({'level': 'low',    'text': '財報不如預期可能引發短期大幅波動，需控管部位'})
    return risks[:5]

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

@app.route('/api/stock/<ticker>')
def get_stock(ticker):
    try:
        ticker = ticker.upper().strip()
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

        # Bollinger current state
        bb_u = safe_float(hist['BB_upper'].iloc[-1])
        bb_m = safe_float(hist['BB_mid'].iloc[-1])
        bb_l = safe_float(hist['BB_lower'].iloc[-1])
        bb_width = round((bb_u - bb_l) / bb_m * 100, 2) if bb_m else 0
        bb_pos   = round((price - bb_l) / (bb_u - bb_l) * 100, 1) if (bb_u - bb_l) else 50

        # Analysis
        levels      = get_levels(hist)
        conclusions = gen_conclusions(price, ma5, ma20, ma60, macd_v, dea_v, rsi_v, vol_ratio)
        catalysts   = gen_catalysts(price, ma5, ma20, ma60, macd_v, dea_v, rsi_v, vol_ratio, week52h, info)
        risks       = gen_risks(price, ma20, rsi_v, vol_ratio, week52h)
        strategy    = gen_strategy(price, ma5, ma20, ma60, rsi_v, levels)
        returns     = calc_returns(hist)

        # Quarterly revenue
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
                                quarterly.append({
                                    'period':  str(col)[:7],
                                    'revenue': round(v / 1e6, 1)
                                })
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

        return jsonify({
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
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/news/<ticker>')
def get_news(ticker):
    try:
        stock     = yf.Ticker(ticker.upper())
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
        return jsonify({'ticker': ticker.upper(), 'articles': articles})
    except Exception as e:
        return jsonify({'ticker': ticker.upper(), 'articles': [], 'error': str(e)})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)

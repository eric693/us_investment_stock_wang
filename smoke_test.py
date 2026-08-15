# -*- coding: utf-8 -*-
"""冒煙測試：上線前跑一次，確認所有 GET 頁面與主要 API 都不是 5xx。

用法（在 us_investment_stock 目錄下）：
    venv/bin/python smoke_test.py            # 只掃頁面與輕量 API（約 1 分鐘）
    venv/bin/python smoke_test.py --full     # 連類股排行等慢端點一起掃（數十秒更久）

不呼叫任何 AI 端點（不花 Token），也不寫入任何設定；純讀取驗證。
回傳碼 0＝全過，1＝有失敗，方便掛進部署流程。
"""
import sys
import time
import json

import app as application

FULL = '--full' in sys.argv

# 需要參數的 API：帶一檔代表性台股與美股
SAMPLE_TW = '2330'
SAMPLE_US = 'AAPL'

# 慢端點（首次計算需數十秒），只在 --full 時測
SLOW = {'/api/market/sectors'}

# 只測 GET；POST 端點多半會改狀態或呼叫 AI，冒煙測試不碰
EXTRA_API = [
    '/api/market/board',
    '/api/market/sectors',
    '/api/watch/quotes',
    '/api/watch/groups',
    '/api/watch/calendar',
    f'/api/predict/analyze/{SAMPLE_TW}',
    f'/api/predict/kline/{SAMPLE_TW}?tw=1&period=3mo&interval=1d',
    f'/api/predict/kline/{SAMPLE_US}?tw=0&period=3mo&interval=1d',
    f'/api/predict/intraday/{SAMPLE_TW}?tw=1',
    f'/api/predict/intraday/{SAMPLE_US}?tw=0',
    f'/api/predict/chips/{SAMPLE_TW}',
    f'/api/tw/news/{SAMPLE_TW}',
    '/api/portfolio/priced',
    '/api/tw/monitor/list',
]

# 這些端點在資料源限流／非交易時段可能回 4xx，屬預期，不算失敗
TOLERATE_4XX = {'/api/tw/intraday/', '/api/tw/hourly/'}

# 關鍵欄位檢查：抓「回 200 但內容壞掉」的情況
FIELD_CHECKS = {
    '/api/market/board':  lambda j: isinstance(j.get('indices'), dict) and 'regime' in j,
    '/api/watch/quotes':  lambda j: isinstance(j.get('rows'), list),
    f'/api/predict/kline/{SAMPLE_TW}?tw=1&period=3mo&interval=1d':
        lambda j: len(j.get('candles') or []) > 10 and 'osc' in (j['candles'][-1] or {}),
    f'/api/predict/intraday/{SAMPLE_TW}?tw=1':
        lambda j: len(j.get('points') or []) > 5,
    f'/api/predict/chips/{SAMPLE_TW}':
        lambda j: isinstance(j.get('rows'), list),
}


def page_routes():
    """所有不需參數的 GET 頁面路徑（排除 API 與登入/登出）。"""
    out = []
    for rule in application.app.url_map.iter_rules():
        if 'GET' not in (rule.methods or set()):
            continue
        path = str(rule)
        if rule.arguments or path.startswith('/api/') or path.startswith('/static'):
            continue
        if path in ('/logout', '/login'):
            continue
        out.append(path)
    return sorted(out)


def main():
    client = application.app.test_client()
    with client.session_transaction() as sess:
        sess['authed'] = True

    targets = page_routes() + EXTRA_API
    fails, warns, ok = [], [], 0

    for path in targets:
        if path in SLOW and not FULL:
            print(f'  skip  {path}  （慢端點，加 --full 才測）')
            continue
        t0 = time.time()
        try:
            resp = client.get(path)
            code = resp.status_code
        except Exception as e:                       # 端點直接丟例外
            fails.append((path, f'例外 {e}'))
            print(f'  FAIL  {path}  例外 {e}')
            continue
        dur = time.time() - t0

        if code >= 500:
            fails.append((path, f'HTTP {code}'))
            print(f'  FAIL  {path}  HTTP {code}  ({dur:.1f}s)')
            continue
        if code >= 400 and not any(path.startswith(p) for p in TOLERATE_4XX):
            warns.append((path, f'HTTP {code}'))
            print(f'  WARN  {path}  HTTP {code}  ({dur:.1f}s)')
            continue

        check = FIELD_CHECKS.get(path)
        if check and code == 200:
            try:
                body = json.loads(resp.get_data(as_text=True)
                                  .replace('NaN', 'null').replace('Infinity', 'null'))
                if not check(body):
                    fails.append((path, '欄位檢查未通過'))
                    print(f'  FAIL  {path}  欄位檢查未通過  ({dur:.1f}s)')
                    continue
            except Exception as e:
                fails.append((path, f'JSON 解析失敗 {e}'))
                print(f'  FAIL  {path}  JSON 解析失敗 {e}')
                continue
        ok += 1
        print(f'  ok    {path}  HTTP {code}  ({dur:.1f}s)')

    print('\n' + '=' * 60)
    print(f'通過 {ok}　警告 {len(warns)}　失敗 {len(fails)}')
    for p, why in warns:
        print(f'  警告：{p} → {why}')
    for p, why in fails:
        print(f'  失敗：{p} → {why}')
    return 1 if fails else 0


if __name__ == '__main__':
    sys.exit(main())

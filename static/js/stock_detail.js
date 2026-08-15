/* ============================================================
 * 全站共用：個股詳情滑出面板（drawer）
 * 用法：openStockDetail(code, isTw)
 *   - 台股：整合 技術/綜合評分 + 籌碼(法人/融資/借券/當沖) + 新聞
 *   - 美股：整合 基本面 + 新聞
 * 只需在頁面引入：<script src="/static/js/stock_detail.js"></script>
 * 不使用任何 emoji。
 * ============================================================ */
(function () {
  if (window.__stockDetailLoaded) return;
  window.__stockDetailLoaded = true;

  // ── 注入樣式（沿用全站 CSS 變數，缺值有 fallback）──────────
  var css = `
  .sd-overlay{position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:9000;opacity:0;transition:opacity .2s;}
  .sd-overlay.open{opacity:1;}
  .sd-drawer{position:fixed;top:0;right:0;height:100%;width:min(560px,94vw);background:var(--card-bg,#0c1219);
    border-left:1px solid var(--card-bd,#1b2838);z-index:9001;transform:translateX(100%);transition:transform .25s ease;
    display:flex;flex-direction:column;box-shadow:-12px 0 40px rgba(0,0,0,.5);}
  .sd-drawer.open{transform:translateX(0);}
  .sd-head{padding:16px 18px;border-bottom:1px solid var(--card-bd,#1b2838);display:flex;align-items:flex-start;gap:10px;}
  .sd-head .sd-titlewrap{flex:1;min-width:0;}
  .sd-code{font-size:1.15rem;font-weight:900;color:var(--text,#e8eef6);}
  .sd-name{font-size:.8rem;color:var(--text-dim,#7d8da3);margin-top:2px;}
  .sd-price{font-size:1.35rem;font-weight:900;color:var(--text,#e8eef6);text-align:right;white-space:nowrap;}
  .sd-grade{display:inline-block;margin-top:3px;padding:2px 9px;border-radius:6px;font-size:.74rem;font-weight:800;}
  .sd-close{background:none;border:none;color:var(--text-dim,#7d8da3);font-size:1.5rem;line-height:1;cursor:pointer;padding:0 4px;}
  .sd-close:hover{color:var(--text,#fff);}
  .sd-tabs{display:flex;gap:4px;padding:10px 14px 0;border-bottom:1px solid var(--card-bd,#1b2838);}
  .sd-tab{padding:7px 14px;border-radius:7px 7px 0 0;border:none;background:transparent;color:var(--text-dim,#7d8da3);
    font-size:.83rem;font-weight:700;cursor:pointer;}
  .sd-tab.active{color:var(--blue,#3d8ef8);background:rgba(61,142,248,.1);}
  .sd-body{flex:1;overflow-y:auto;padding:16px 18px;}
  .sd-pane{display:none;}
  .sd-pane.active{display:block;}
  .sd-sec{font-size:.72rem;font-weight:800;color:var(--text-dim,#7d8da3);letter-spacing:.5px;margin:16px 0 8px;text-transform:uppercase;}
  .sd-sec:first-child{margin-top:0;}
  .sd-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;}
  .sd-kv{background:var(--bg2,#0a1320);border:1px solid var(--card-bd,#1b2838);border-radius:8px;padding:8px 11px;}
  .sd-k{font-size:.7rem;color:var(--text-dim,#7d8da3);}
  .sd-v{font-size:.92rem;font-weight:800;color:var(--text,#e8eef6);margin-top:2px;}
  .sd-v small{font-size:.7rem;font-weight:600;color:var(--text-mid,#a9b6c8);}
  .sd-pos{color:var(--red,#e84646);}      /* 台股紅漲 */
  .sd-neg{color:var(--green,#00d68f);}    /* 台股綠跌 */
  .sd-up{color:var(--green,#00d68f);}     /* 美股綠漲 */
  .sd-down{color:var(--red,#e84646);}
  .sd-fac{display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid rgba(27,40,56,.5);font-size:.82rem;}
  .sd-fac .dot{width:8px;height:8px;border-radius:50%;flex-shrink:0;}
  .sd-fac .dot.ok{background:var(--green,#00d68f);}
  .sd-fac .dot.no{background:var(--text-dim,#555);}
  .sd-fac .fl{flex:1;color:var(--text,#e8eef6);font-weight:700;}
  .sd-fac .fd{color:var(--text-dim,#7d8da3);font-size:.74rem;}
  .sd-news{display:block;padding:10px 0;border-bottom:1px solid rgba(27,40,56,.5);text-decoration:none;}
  .sd-news .nt{font-size:.86rem;font-weight:700;color:var(--text,#e8eef6);line-height:1.4;}
  .sd-news:hover .nt{color:var(--blue,#3d8ef8);}
  .sd-news .nm{font-size:.7rem;color:var(--text-dim,#7d8da3);margin-top:3px;}
  .sd-foot{padding:12px 18px;border-top:1px solid var(--card-bd,#1b2838);display:flex;gap:8px;}
  .sd-btn{flex:1;padding:9px;border-radius:8px;border:1px solid var(--card-bd2,#28384c);background:rgba(255,255,255,.05);
    color:var(--text,#e8eef6);font-size:.82rem;font-weight:700;cursor:pointer;text-align:center;text-decoration:none;}
  .sd-btn:hover{background:rgba(255,255,255,.1);}
  .sd-btn.primary{background:var(--accent,#1a6fd4);border-color:transparent;color:#fff;}
  .sd-btn.primary:hover{background:#2278e0;}
  .sd-loading,.sd-empty{text-align:center;color:var(--text-dim,#7d8da3);padding:30px;font-size:.86rem;}
  .sd-legend{display:flex;gap:14px;flex-wrap:wrap;font-size:.7rem;color:var(--text-dim,#7d8da3);margin-bottom:8px;}
  .sd-legend i{display:inline-block;width:12px;height:3px;border-radius:2px;margin-right:4px;vertical-align:middle;}
  .sd-chartwrap{width:100%;background:var(--bg2,#0a1320);border:1px solid var(--card-bd,#1b2838);border-radius:8px;padding:6px 4px;}
  .sd-canvas{display:block;width:100%;}
  .sd-cap{margin-top:10px;font-size:.8rem;line-height:1.55;color:var(--text-mid,#a9b6c8);
    background:var(--bg2,#0a1320);border:1px solid var(--card-bd,#1b2838);border-radius:8px;padding:10px 12px;}
  .sd-cap b{color:var(--text,#e8eef6);}
  .sd-capb{font-size:.9rem;margin-bottom:6px;padding-bottom:6px;border-bottom:1px solid rgba(27,40,56,.7);}
  .sd-capnote{margin-top:8px;}
  .sd-tf{display:flex;gap:6px;margin-bottom:8px;}
  .sd-tfbtn{padding:5px 12px;border-radius:7px;border:1px solid var(--card-bd2,#28384c);background:rgba(255,255,255,.04);
    color:var(--text-dim,#7d8da3);font-size:.76rem;font-weight:700;cursor:pointer;}
  .sd-tfbtn:hover{color:var(--text,#e8eef6);}
  .sd-tfbtn.active{background:rgba(61,142,248,.16);border-color:transparent;color:var(--blue,#3d8ef8);}
  .sd-chartwrap{position:relative;}
  .sd-canvas{cursor:crosshair;}
  .sd-hover{font-size:.72rem;color:var(--text-mid,#a9b6c8);margin-bottom:6px;min-height:16px;
    display:flex;gap:10px;flex-wrap:wrap;}
  .sd-hover b{color:var(--text,#e8eef6);font-weight:800;}
  .sd-tbl{width:100%;border-collapse:collapse;font-size:.75rem;}
  .sd-tbl th{color:var(--text-dim,#7d8da3);font-weight:700;text-align:right;padding:5px 6px;
    border-bottom:1px solid var(--card-bd,#1b2838);white-space:nowrap;}
  .sd-tbl th:first-child,.sd-tbl td:first-child{text-align:left;}
  .sd-tbl td{text-align:right;padding:5px 6px;border-bottom:1px solid rgba(27,40,56,.5);
    color:var(--text,#e8eef6);white-space:nowrap;font-variant-numeric:tabular-nums;}
  .sd-tblwrap{overflow-x:auto;}
  .sd-tbl tr.sum td,.sd-tbl tr.sum th{background:rgba(61,142,248,.07);font-weight:800;}
  `;
  var st = document.createElement('style'); st.textContent = css; document.head.appendChild(st);

  // ── 建立 DOM（單例）────────────────────────────────────────
  var overlay, drawer, elHead, elTabs, elBody, elFoot, state = {};
  function build() {
    overlay = document.createElement('div'); overlay.className = 'sd-overlay';
    overlay.onclick = closeDetail;
    drawer = document.createElement('div'); drawer.className = 'sd-drawer';
    drawer.onclick = function (e) { e.stopPropagation(); };
    elHead = document.createElement('div'); elHead.className = 'sd-head';
    elTabs = document.createElement('div'); elTabs.className = 'sd-tabs';
    elBody = document.createElement('div'); elBody.className = 'sd-body';
    elFoot = document.createElement('div'); elFoot.className = 'sd-foot';
    drawer.appendChild(elHead); drawer.appendChild(elTabs); drawer.appendChild(elBody); drawer.appendChild(elFoot);
    document.body.appendChild(overlay); document.body.appendChild(drawer);
    document.addEventListener('keydown', function (e) { if (e.key === 'Escape') closeDetail(); });
  }

  function closeDetail() {
    if (!drawer) return;
    drawer.classList.remove('open'); overlay.classList.remove('open');
    setTimeout(function () { overlay.style.display = 'none'; }, 250);
  }

  // 後端 jsonify 在收盤/週末可能輸出 NaN/Infinity（非合法 JSON，瀏覽器 JSON.parse 會丟錯）。
  // 取文字後先清成 null 再 parse，確保面板永不因此整個失效。
  function getJson(url) {
    return fetch(url).then(function (r) { return r.text(); }).then(function (t) {
      return JSON.parse(t.replace(/\bNaN\b/g, 'null').replace(/-?\bInfinity\b/g, 'null'));
    });
  }

  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"]/g, function (c) {
    return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]; }); }
  function num(v, dash) { return (v === null || v === undefined || v === '' || isNaN(v)) ? (dash || '—') : v; }

  // ── 對外主函式 ─────────────────────────────────────────────
  window.openStockDetail = function (code, isTw) {
    if (!drawer) build();
    code = String(code || '').trim().toUpperCase().replace('.TW', '').replace('.TWO', '');
    if (!code) return;
    if (isTw === undefined) isTw = !/[A-Z]/.test(code) || /^00/.test(code); // 數字代碼視為台股
    state = { code: code, isTw: !!isTw, tab: 'kline', data: null, news: null, kline: null, ktf: { period: '3mo', interval: '1d' } };
    overlay.style.display = 'block';
    requestAnimationFrame(function () { overlay.classList.add('open'); drawer.classList.add('open'); });
    elHead.innerHTML = '<div class="sd-titlewrap"><div class="sd-code">' + esc(code) + '</div>'
      + '<div class="sd-name">載入中…</div></div>'
      + '<button class="sd-close" title="關閉">&times;</button>';
    elHead.querySelector('.sd-close').onclick = closeDetail;
    elTabs.innerHTML = ''; elFoot.innerHTML = '';
    elBody.innerHTML = '<div class="sd-loading">載入中，請稍候…</div>';
    state.isTw ? loadTw(code) : loadUs(code);
  };

  // ════════════ 台股 ════════════
  function loadTw(code) {
    Promise.all([
      getJson('/api/predict/analyze/' + code).catch(function () { return { error: '分析資料載入失敗' }; }),
      getJson('/api/tw/news/' + code).catch(function () { return { articles: [] }; }),
      getJson('/api/predict/kline/' + code + '?tw=1&period=3mo&interval=1d').catch(function () { return { error: 'K線載入失敗' }; })
    ]).then(function (arr) {
      state.data = arr[0]; state.news = (arr[1] && arr[1].articles) || []; state.kline = arr[2];
      renderTwHead(); renderTwTabs(); renderTwFoot(); renderTab();
    });
  }

  function gradeColor(g) {
    return { A: 'background:rgba(0,214,143,.15);color:#00d68f', B: 'background:rgba(61,142,248,.15);color:#3d8ef8',
      C: 'background:rgba(240,180,41,.15);color:#f0b429', D: 'background:rgba(232,120,70,.15);color:#e87846',
      F: 'background:rgba(232,70,70,.15);color:#e84646' }[g] || 'background:rgba(125,141,163,.15);color:#7d8da3';
  }

  // 規則式明日傾向（不呼叫 AI、不耗 token）；台股紅漲綠跌。回傳 5 項內訳供 K線說明逐項展開。
  function twTomorrowBias(d) {
    if (d.price == null || d.ma20 == null) return null;
    var items = [
      { ok: d.price > d.ma20, label: '站上月線(20MA)', d: '現價 ' + d.price + ' / MA20 ' + num(d.ma20) },
      { ok: (d.osc || 0) > 0, label: 'MACD 動能偏多', d: 'OSC ' + num(d.osc) + (d.osc_trend ? '（' + d.osc_trend + '）' : '') },
      { ok: (d.k || 0) > (d.d || 0), label: 'KD 向上', d: 'K ' + num(d.k) + ' / D ' + num(d.d) + (d.kd_status ? '（' + d.kd_status + '）' : '') },
      { ok: (d.rsi || 50) > 50, label: 'RSI 偏強', d: 'RSI ' + num(d.rsi) },
      { ok: !!d.ma_bull, label: '均線多頭排列', d: d.ma_bull ? 'MA5>MA20>MA60' : '尚未多頭排列' }
    ];
    var pts = items.reduce(function (s, i) { return s + (i.ok ? 1 : 0); }, 0);
    var L = pts >= 4 ? ['看漲', 'rgba(232,70,70,.18)', '#e84646']
      : pts === 3 ? ['偏多', 'rgba(232,120,70,.16)', '#e87846']
      : pts === 2 ? ['中性', 'rgba(240,180,41,.16)', '#f0b429']
      : pts === 1 ? ['偏空', 'rgba(0,214,143,.14)', '#00d68f']
      : ['看跌', 'rgba(0,214,143,.2)', '#00d68f'];
    return { label: L[0], pts: pts, style: 'background:' + L[1] + ';color:' + L[2], items: items };
  }

  function renderTwHead(d) {
    d = state.data || {};
    var mf = d.mf_score || {};
    var grade = mf.grade ? '<span class="sd-grade" style="' + gradeColor(mf.grade) + '">綜合評分 ' + esc(mf.grade)
      + ' · ' + num(mf.pct, '?') + '%</span>' : '';
    var b = twTomorrowBias(d);
    var bias = b ? '<span class="sd-grade" style="' + b.style + ';margin-left:6px">明日傾向 ' + b.label + '（' + b.pts + '/5）</span>' : '';
    elHead.innerHTML = '<div class="sd-titlewrap"><div class="sd-code">' + esc(d.name || state.code)
      + ' <small style="color:var(--text-dim);font-weight:600">' + esc(state.code) + (d.is_etf ? ' · ETF' : '') + '</small></div>'
      + '<div class="sd-name">台股 · 即時技術與籌碼</div>' + grade + bias + '</div>'
      + '<div><div class="sd-price">' + (d.price != null ? d.price : '—') + '</div></div>'
      + '<button class="sd-close" title="關閉">&times;</button>';
    elHead.querySelector('.sd-close').onclick = closeDetail;
  }

  // 分頁以名稱（字串）辨識，不用索引，之後增減分頁不會錯位
  function renderTabsBar(tabs, rerender) {
    elTabs.innerHTML = tabs.map(function (t) {
      return '<button class="sd-tab' + (t[0] === state.tab ? ' active' : '') + '" data-k="' + t[0] + '">' + t[1] + '</button>';
    }).join('');
    Array.prototype.forEach.call(elTabs.querySelectorAll('.sd-tab'), function (b) {
      b.onclick = function () { state.tab = b.dataset.k; rerender(); renderTab(); };
    });
  }
  function renderTwTabs() {
    renderTabsBar([['kline', 'K線'], ['intraday', '分時'], ['tech', '技術 / 起漲'],
                   ['chips', '籌碼'], ['news', '新聞']], renderTwTabs);
  }

  function kv(k, v, cls) { return '<div class="sd-kv"><div class="sd-k">' + k + '</div><div class="sd-v ' + (cls || '') + '">' + v + '</div></div>'; }

  function renderTab() {
    if (state.tab === 'kline') return renderKline();
    if (state.tab === 'intraday') return renderIntraday();
    if (state.isTw) {
      var d = state.data || {};
      if (d.error) { elBody.innerHTML = '<div class="sd-empty">' + esc(d.error) + '</div>'; return; }
      if (state.tab === 'tech') return renderTwTech(d);
      if (state.tab === 'chips') return renderTwChips(d);
      return renderNews();
    }
    if (state.tab === 'fund') return renderUsFund();
    return renderNews();
  }

  // ── K 線（自繪 canvas 蠟燭圖，不依賴任何外部圖表庫，全站詳情面板共用）──
  // 可切換 日K·3月 / 日K·6月 / 週K（不耗 token，純行情）。
  function klineToolbar() {
    var tf = state.ktf || { period: '6mo', interval: '1d' };
    var opts = [['6mo', '1d', '日K·6月'], ['3mo', '1d', '日K·3月'], ['2y', '1wk', '週K']];
    return '<div class="sd-tf">' + opts.map(function (o) {
      var active = (o[1] === '1wk') ? (tf.interval === '1wk') : (tf.interval === '1d' && tf.period === o[0]);
      return '<button class="sd-tfbtn' + (active ? ' active' : '') + '" data-p="' + o[0] + '" data-i="' + o[1] + '">' + o[2] + '</button>';
    }).join('') + '</div>' + subToolbar();
  }
  // 副圖切換（MACD / KD / RSI），純前端重畫、不重抓資料
  function subToolbar() {
    var cur = state.ksub || 'macd';
    var opts = [['macd', 'MACD'], ['kd', 'KD'], ['rsi', 'RSI'], ['none', '不顯示副圖']];
    return '<div class="sd-tf sd-subtf">' + opts.map(function (o) {
      return '<button class="sd-tfbtn' + (cur === o[0] ? ' active' : '') + '" data-sub="' + o[0] + '">' + o[1] + '</button>';
    }).join('') + '</div>';
  }
  function bindTf() {
    Array.prototype.forEach.call(elBody.querySelectorAll('.sd-tfbtn'), function (b) {
      b.onclick = b.dataset.sub
        ? function () { state.ksub = b.dataset.sub; renderKline(); }
        : function () { reloadKline(b.dataset.p, b.dataset.i); };
    });
  }
  function reloadKline(period, interval) {
    state.ktf = { period: period, interval: interval };
    state.kline = null;
    renderKline();   // 先顯示載入中＋切換鈕高亮
    var url = '/api/predict/kline/' + encodeURIComponent(state.code)
      + '?tw=' + (state.isTw ? '1' : '0') + '&period=' + period + '&interval=' + interval;
    getJson(url).catch(function () { return { error: 'K線載入失敗' }; }).then(function (k) {
      // 使用者可能在載入期間切到別的時間框，只在仍是這個請求時套用
      if (state.ktf.period === period && state.ktf.interval === interval) {
        state.kline = k;
        if (state.tab === 'kline') renderKline();
      }
    });
  }

  function renderKline() {
    var tb = klineToolbar();
    var k = state.kline;
    if (!k) { elBody.innerHTML = tb + '<div class="sd-loading">K 線載入中…</div>'; bindTf(); return; }
    var candles = k.candles || [];
    if (k.error && !candles.length) { elBody.innerHTML = tb + '<div class="sd-empty">' + esc(k.error) + '</div>'; bindTf(); return; }
    if (!candles.length) { elBody.innerHTML = tb + '<div class="sd-empty">暫無 K 線資料</div>'; bindTf(); return; }
    var tfLabel = (k.interval === '1wk') ? '週K' : '日K';
    var hasYear = candles.some(function (c) { return c.myear != null; });
    var legend = '<div class="sd-legend">'
      + '<span><i style="background:#f0b429"></i>MA5</span>'
      + '<span><i style="background:#3d8ef8"></i>MA20</span>'
      + '<span><i style="background:#b06fe0"></i>MA60</span>'
      + (hasYear ? '<span><i style="background:#d7e2f0;height:2px"></i>' + esc(k.year_label || '年線') + '</span>' : '')
      + (state.ksub === 'kd' ? '<span><i style="background:#f0b429"></i>K<i style="background:#3d8ef8;margin-left:6px"></i>D</span>'
        : state.ksub === 'rsi' ? '<span><i style="background:#b06fe0"></i>RSI(14)</span>'
        : state.ksub === 'none' ? ''
        : '<span><i style="background:#f0b429"></i>DIF<i style="background:#3d8ef8;margin-left:6px"></i>DEA</span>')
      + '<span style="margin-left:auto">' + tfLabel + ' ' + esc(candles[0].t) + ' ~ ' + esc(candles[candles.length - 1].t) + '</span></div>';
    elBody.innerHTML = tb + legend + '<div class="sd-hover"></div>'
      + '<div class="sd-chartwrap"><canvas class="sd-canvas"></canvas></div>' + klineCaption();
    bindTf();
    var cv = elBody.querySelector('.sd-canvas'), hv = elBody.querySelector('.sd-hover');
    // 抽屜剛開時可能寬度尚未定案，下一幀再量一次確保正確
    requestAnimationFrame(function () { drawCandles(cv, candles, state.isTw); showHover(hv, candles, candles.length - 1); });
    // 十字游標：滑鼠／觸控移到哪一根，就顯示那根的 OHLC 量與均線值（三竹式看盤基本操作）
    function idxAt(clientX) {
      var r = cv.getBoundingClientRect(), padL = 46, padR = 8;
      var plotW = r.width - padL - padR, cw = plotW / candles.length;
      var i = Math.floor((clientX - r.left - padL) / cw);
      return Math.max(0, Math.min(candles.length - 1, i));
    }
    function move(e) {
      var x = (e.touches && e.touches[0] ? e.touches[0].clientX : e.clientX);
      var i = idxAt(x);
      drawCandles(cv, candles, state.isTw, i); showHover(hv, candles, i);
    }
    cv.onmousemove = move;
    cv.ontouchstart = cv.ontouchmove = function (e) { move(e); e.preventDefault(); };
    cv.onmouseleave = function () {
      drawCandles(cv, candles, state.isTw); showHover(hv, candles, candles.length - 1);
    };
  }

  // ── 分時走勢（今日 5 分 K；非交易時段自動顯示最近一個交易日）──
  function renderIntraday() {
    var d = state.intraday;
    if (d === undefined) {                       // 首次進入分頁才載入，不拖慢開啟速度
      state.intraday = null;
      elBody.innerHTML = '<div class="sd-loading">分時資料載入中…</div>';
      var code = state.code;
      getJson('/api/predict/intraday/' + encodeURIComponent(code) + '?tw=' + (state.isTw ? '1' : '0'))
        .catch(function () { return { error: '分時載入失敗' }; })
        .then(function (r) {
          if (state.code !== code) return;       // 使用者已切換到別檔
          state.intraday = r;
          if (state.tab === 'intraday') renderIntraday();
        });
      return;
    }
    if (!d) { elBody.innerHTML = '<div class="sd-loading">分時資料載入中…</div>'; return; }
    var pts = d.points || [];
    if (!pts.length) {
      elBody.innerHTML = '<div class="sd-empty">' + esc(d.error || '暫無分時資料') + '</div>';
      return;
    }
    var upCls = state.isTw ? 'sd-pos' : 'sd-up', dnCls = state.isTw ? 'sd-neg' : 'sd-down';
    var chgCls = (d.change_pct || 0) >= 0 ? upCls : dnCls;
    var head = '<div class="sd-legend">'
      + '<span><i style="background:#3d8ef8"></i>成交價</span>'
      + '<span><i style="background:#f0b429"></i>均價</span>'
      + (d.prev_close ? '<span><i style="background:#7d8da3;height:2px"></i>昨收 ' + num(d.prev_close) + '</span>' : '')
      + '<span style="margin-left:auto">' + esc(d.date || '') + (d.is_last_session ? '（最近交易日）' : '（今日）') + '</span></div>';
    var stat = '<div class="sd-hover"><span>最新 <b>' + num(d.last) + '</b></span>'
      + (d.change_pct != null ? '<span class="' + chgCls + '">' + (d.change >= 0 ? '+' : '') + num(d.change)
          + '（' + (d.change_pct >= 0 ? '+' : '') + num(d.change_pct) + '%）</span>' : '')
      + '<span>高 <b>' + num(d.high) + '</b></span><span>低 <b>' + num(d.low) + '</b></span>'
      + '<span>均價 <b>' + num(pts[pts.length - 1].avg) + '</b></span></div>';
    elBody.innerHTML = head + stat + '<div class="sd-hover" id="sdIntraHover"></div>'
      + '<div class="sd-chartwrap"><canvas class="sd-canvas"></canvas></div>'
      + '<div class="sd-cap"><div class="sd-capnote">價格在均價之上代表當日買方掌控（盤中偏強），'
      + '跌破均價且量放大要留意賣壓。灰色虛線為昨收，站上為紅盤、跌破為綠盤。</div></div>';
    var cv = elBody.querySelector('.sd-canvas'), hv = document.getElementById('sdIntraHover');
    requestAnimationFrame(function () { drawIntraday(cv, d, null); });
    function move(e) {
      var x = (e.touches && e.touches[0] ? e.touches[0].clientX : e.clientX);
      var r = cv.getBoundingClientRect(), padL = 46, padR = 8;
      var i = Math.floor((x - r.left - padL) / ((r.width - padL - padR) / pts.length));
      i = Math.max(0, Math.min(pts.length - 1, i));
      drawIntraday(cv, d, i);
      var p = pts[i];
      hv.innerHTML = '<span><b>' + esc(p.t) + '</b></span><span>價 <b>' + num(p.p) + '</b></span>'
        + '<span>均價 ' + num(p.avg) + '</span><span>量 ' + (p.v || 0).toLocaleString() + '</span>';
    }
    cv.onmousemove = move;
    cv.ontouchstart = cv.ontouchmove = function (e) { move(e); e.preventDefault(); };
    cv.onmouseleave = function () { drawIntraday(cv, d, null); hv.innerHTML = ''; };
  }

  function drawIntraday(canvas, d, hoverIdx) {
    var pts = d.points || [];
    if (!canvas || !pts.length) return;
    var W = canvas.parentNode.clientWidth || 480, H = 280, dpr = window.devicePixelRatio || 1;
    canvas.width = W * dpr; canvas.height = H * dpr; canvas.style.height = H + 'px';
    var ctx = canvas.getContext('2d'); ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, W, H);
    var padL = 46, padR = 8, padT = 6, padB = 16, gap = 8, volH = Math.round(H * 0.2);
    var priceH = H - volH - gap - padT - padB, priceTop = padT, volTop = padT + priceH + gap;
    var plotW = W - padL - padR, n = pts.length, cw = plotW / n;
    var hi = -Infinity, lo = Infinity;
    pts.forEach(function (p) { hi = Math.max(hi, p.p, p.avg); lo = Math.min(lo, p.p, p.avg); });
    if (d.prev_close) { hi = Math.max(hi, d.prev_close); lo = Math.min(lo, d.prev_close); }
    var pad = (hi - lo) * 0.08 || 1; hi += pad; lo -= pad;
    function y(v) { return priceTop + (hi - v) / (hi - lo) * priceH; }
    ctx.font = '10px -apple-system,system-ui,sans-serif'; ctx.textBaseline = 'middle';
    for (var i = 0; i <= 4; i++) {
      var v = lo + (hi - lo) * i / 4, yy = y(v);
      ctx.strokeStyle = 'rgba(125,141,163,.13)'; ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(padL, yy); ctx.lineTo(W - padR, yy); ctx.stroke();
      ctx.fillStyle = '#7d8da3'; ctx.fillText(v.toFixed(1), 4, yy);
    }
    // 昨收基準線（分時圖的紅綠分界）
    if (d.prev_close) {
      var py = y(d.prev_close);
      ctx.save(); ctx.strokeStyle = 'rgba(125,141,163,.6)'; ctx.setLineDash([4, 4]);
      ctx.beginPath(); ctx.moveTo(padL, py); ctx.lineTo(W - padR, py); ctx.stroke(); ctx.restore();
    }
    function line(key, color, w) {
      ctx.strokeStyle = color; ctx.lineWidth = w || 1.4; ctx.beginPath();
      pts.forEach(function (p, i) {
        var cx = padL + cw * i + cw / 2, yy = y(p[key]);
        i ? ctx.lineTo(cx, yy) : ctx.moveTo(cx, yy);
      });
      ctx.stroke();
    }
    line('p', '#3d8ef8', 1.6); line('avg', '#f0b429', 1.2);
    var maxV = Math.max.apply(null, pts.map(function (p) { return p.v || 0; })) || 1;
    var up = state.isTw ? '#e84646' : '#00d68f', dn = state.isTw ? '#00d68f' : '#e84646';
    ctx.globalAlpha = .5;
    pts.forEach(function (p, i) {
      var cx = padL + cw * i + cw / 2, vh = (p.v || 0) / maxV * volH;
      var prev = i > 0 ? pts[i - 1].p : p.p;
      ctx.fillStyle = p.p >= prev ? up : dn;
      ctx.fillRect(cx - Math.max(1, cw * 0.6) / 2, volTop + volH - vh, Math.max(1, cw * 0.6), vh);
    });
    ctx.globalAlpha = 1;
    // 時間刻度（每約 1/4 標一個）
    ctx.fillStyle = '#7d8da3';
    [0, Math.floor(n / 4), Math.floor(n / 2), Math.floor(n * 3 / 4), n - 1].forEach(function (i) {
      if (!pts[i]) return;
      ctx.fillText(pts[i].t, Math.min(W - 34, Math.max(padL, padL + cw * i)), H - 6);
    });
    if (hoverIdx != null && pts[hoverIdx]) {
      var hx = padL + cw * hoverIdx + cw / 2, hy = y(pts[hoverIdx].p);
      ctx.save(); ctx.strokeStyle = 'rgba(180,200,225,.55)'; ctx.setLineDash([3, 3]);
      ctx.beginPath(); ctx.moveTo(hx, padT); ctx.lineTo(hx, volTop + volH); ctx.stroke();
      ctx.beginPath(); ctx.moveTo(padL, hy); ctx.lineTo(W - padR, hy); ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = 'rgba(20,30,44,.95)'; ctx.fillRect(0, hy - 8, padL - 4, 16);
      ctx.fillStyle = '#e8eef6'; ctx.fillText(pts[hoverIdx].p.toFixed(1), 4, hy);
      ctx.restore();
    }
  }

  // 十字游標資訊列：日期 / 開高低收 / 漲跌幅 / 量 / 均線
  function showHover(el, candles, i) {
    if (!el) return;
    var c = candles[i]; if (!c) { el.innerHTML = ''; return; }
    var prev = i > 0 ? candles[i - 1] : null;
    var chg = prev && prev.c ? (c.c - prev.c) / prev.c * 100 : null;
    var chgCls = chg == null ? '' : (state.isTw ? (chg >= 0 ? 'sd-pos' : 'sd-neg')
                                                : (chg >= 0 ? 'sd-up' : 'sd-down'));
    var vol = c.v >= 1e8 ? (c.v / 1e8).toFixed(2) + '億' : (c.v >= 1e4 ? Math.round(c.v / 1e4) + '萬' : c.v);
    var h = '<span><b>' + esc(c.t) + '</b></span>'
      + '<span>開 <b>' + num(c.o) + '</b></span><span>高 <b>' + num(c.h) + '</b></span>'
      + '<span>低 <b>' + num(c.l) + '</b></span><span>收 <b>' + num(c.c) + '</b></span>';
    if (chg != null) h += '<span class="' + chgCls + '">' + (chg >= 0 ? '+' : '') + chg.toFixed(2) + '%</span>';
    h += '<span>量 <b>' + vol + '</b></span>';
    if (c.m5 != null)  h += '<span>MA5 ' + num(c.m5) + '</span>';
    if (c.m20 != null) h += '<span>MA20 ' + num(c.m20) + '</span>';
    if (c.m60 != null) h += '<span>MA60 ' + num(c.m60) + '</span>';
    if (c.myear != null) h += '<span>年線 ' + num(c.myear) + '</span>';
    var sub = state.ksub || 'macd';
    if (sub === 'macd' && c.osc != null) h += '<span>DIF ' + num(c.dif) + ' / DEA ' + num(c.dea) + ' / 柱 ' + num(c.osc) + '</span>';
    if (sub === 'kd' && c.k != null)     h += '<span>K ' + num(c.k) + ' / D ' + num(c.d) + '</span>';
    if (sub === 'rsi' && c.rsi != null)  h += '<span>RSI ' + num(c.rsi) + '</span>';
    el.innerHTML = h;
  }

  // 規則式技術解讀（不耗 token）：明日傾向 5 項逐條展開 + 環境摘要。台股紅漲綠跌。
  function klineCaption() {
    var d = state.data || {};
    if (!state.isTw || d.error || d.price == null) return '';
    var b = twTomorrowBias(d);
    var h = '<div class="sd-cap">';
    if (b) {
      h += '<div class="sd-capb"><b>明日傾向：' + b.label + '（' + b.pts + '/5 項偏多）</b></div>';
      h += b.items.map(function (it) {
        return '<div class="sd-fac"><span class="dot ' + (it.ok ? 'ok' : 'no') + '"></span>'
          + '<span class="fl">' + esc(it.label) + '</span><span class="fd">' + esc(it.d) + '</span></div>';
      }).join('');
    }
    // 年線位階（低位階永動機判斷主軸）：現價相對年線 MA240
    var k = state.kline || {};
    if (k.year_ma && d.price != null) {
      var ya = +k.year_ma, diff = (d.price - ya) / ya * 100;
      var stance = diff >= 0 ? '站上' + (k.year_label || '年線') + '（偏多/高位階）'
        : '位於' + (k.year_label || '年線') + '之下（低位階，符合永動機低接邏輯）';
      h += '<div class="sd-capnote"><b>年線位階：</b>現價 ' + d.price + ' vs ' + esc(k.year_label || '年線') + ' ' + num(ya)
        + '，' + stance + '（乖離 ' + (diff >= 0 ? '+' : '') + diff.toFixed(1) + '%）。</div>';
    }
    var km = (state.kline || {}).marks || [];
    if (km.length) {
      h += '<div class="sd-capnote"><b>我的買賣點：</b>'
        + km.map(function (m) { return esc(m.t) + ' ' + esc(m.label); }).join('；')
        + '（圖上 B＝買進、S＝賣出；黃色虛線為持倉成本）</div>';
    }
    var seg = [];
    if (d.ma20_trend) seg.push('月線方向「' + esc(d.ma20_trend) + '」');
    if (d.bias20 != null) seg.push('乖離率 ' + num(d.bias20) + '%');
    if (d.vol_structure) seg.push('量能' + esc(d.vol_structure));
    if (d.w52_pct != null) seg.push('52週位階 ' + num(d.w52_pct) + '%');
    if (d.rebound_pct != null) seg.push('距低點反彈 ' + num(d.rebound_pct) + '%');
    if (seg.length) h += '<div class="sd-capnote">' + seg.join('、') + '。</div>';
    h += '</div>';
    return h;
  }

  function drawCandles(canvas, candles, isTw, hoverIdx) {
    var wrap = canvas.parentNode;
    var sub = state.ksub || 'macd';
    var subKey = sub === 'macd' ? 'osc' : (sub === 'kd' ? 'k' : 'rsi');
    var hasSub = sub !== 'none' && candles.some(function (c) { return c[subKey] != null; });
    var W = wrap.clientWidth || 480, H = hasSub ? 380 : 300;
    var dpr = window.devicePixelRatio || 1;
    canvas.width = W * dpr; canvas.height = H * dpr;
    canvas.style.height = H + 'px';
    var ctx = canvas.getContext('2d'); ctx.scale(dpr, dpr);
    var padL = 46, padR = 8, padT = 6, padB = 16, gap = 8;
    var volH = Math.round(H * (hasSub ? 0.15 : 0.2));
    var subH = hasSub ? Math.round(H * 0.22) : 0;
    var priceH = H - volH - subH - gap * (hasSub ? 2 : 1) - padT - padB;
    var priceTop = padT, volTop = padT + priceH + gap, subTop = volTop + volH + gap;
    var plotW = W - padL - padR, n = candles.length, cw = plotW / n, bodyW = Math.max(1, cw * 0.62);
    var up = isTw ? '#e84646' : '#00d68f', down = isTw ? '#00d68f' : '#e84646';
    var hi = -Infinity, lo = Infinity;
    candles.forEach(function (c) {
      hi = Math.max(hi, c.h); lo = Math.min(lo, c.l);
      ['m5', 'm20', 'm60', 'myear'].forEach(function (m) { if (c[m] != null) { hi = Math.max(hi, c[m]); lo = Math.min(lo, c[m]); } });
    });
    var pad = (hi - lo) * 0.06 || 1; hi += pad; lo -= pad;
    function y(p) { return priceTop + (hi - p) / (hi - lo) * priceH; }
    ctx.font = '10px -apple-system,system-ui,sans-serif'; ctx.textBaseline = 'middle';
    for (var i = 0; i <= 4; i++) {
      var p = lo + (hi - lo) * i / 4, yy = y(p);
      ctx.strokeStyle = 'rgba(125,141,163,.13)'; ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(padL, yy); ctx.lineTo(W - padR, yy); ctx.stroke();
      ctx.fillStyle = '#7d8da3'; ctx.fillText(p.toFixed(1), 4, yy);
    }
    candles.forEach(function (c, i) {
      var cx = padL + cw * i + cw / 2, col = c.c >= c.o ? up : down;
      ctx.strokeStyle = col; ctx.fillStyle = col; ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(cx, y(c.h)); ctx.lineTo(cx, y(c.l)); ctx.stroke();
      var yo = y(c.o), yc = y(c.c), top = Math.min(yo, yc), bh = Math.max(1, Math.abs(yc - yo));
      ctx.fillRect(cx - bodyW / 2, top, bodyW, bh);
    });
    function maLine(key, color, width) {
      ctx.strokeStyle = color; ctx.lineWidth = width || 1.2; ctx.beginPath(); var started = false;
      candles.forEach(function (c, i) {
        if (c[key] == null) return; var cx = padL + cw * i + cw / 2, yy = y(c[key]);
        started ? ctx.lineTo(cx, yy) : (ctx.moveTo(cx, yy), started = true);
      });
      ctx.stroke();
    }
    maLine('myear', '#d7e2f0', 1.8);   // 年線最粗，作為低位階判斷主軸
    maLine('m5', '#f0b429'); maLine('m20', '#3d8ef8'); maLine('m60', '#b06fe0');

    // ── 個人化：持倉成本線 ＋ 自己的買賣點標記（回頭檢討賣飛/追高最直觀）──
    var kd = state.kline || {};
    if (kd.cost_line != null && kd.cost_line >= lo && kd.cost_line <= hi) {
      var cy = y(kd.cost_line);
      ctx.save();
      ctx.strokeStyle = '#f0b429'; ctx.lineWidth = 1; ctx.setLineDash([5, 4]);
      ctx.beginPath(); ctx.moveTo(padL, cy); ctx.lineTo(W - padR, cy); ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = '#f0b429'; ctx.font = '10px -apple-system,system-ui,sans-serif';
      ctx.fillText('成本 ' + kd.cost_line, padL + 3, cy - 7);
      ctx.restore();
    }
    (kd.marks || []).forEach(function (mk) {
      var mi = -1;
      for (var j = 0; j < candles.length; j++) { if (candles[j].t >= mk.t) { mi = j; break; } }
      if (mi < 0 || mk.p < lo || mk.p > hi) return;      // 不在目前顯示區間就不畫
      var mx = padL + cw * mi + cw / 2, my = y(mk.p), isBuy = mk.side === 'buy';
      ctx.save();
      ctx.fillStyle = isBuy ? '#e84646' : '#00d68f';     // 買紅賣綠（台股慣例）
      ctx.beginPath();
      if (isBuy) { ctx.moveTo(mx, my + 9); ctx.lineTo(mx - 5, my + 17); ctx.lineTo(mx + 5, my + 17); }
      else       { ctx.moveTo(mx, my - 9); ctx.lineTo(mx - 5, my - 17); ctx.lineTo(mx + 5, my - 17); }
      ctx.closePath(); ctx.fill();
      ctx.font = 'bold 9px -apple-system,system-ui,sans-serif';
      ctx.fillText(isBuy ? 'B' : 'S', mx - 3, isBuy ? my + 25 : my - 23);
      ctx.restore();
    });
    var maxV = Math.max.apply(null, candles.map(function (c) { return c.v || 0; })) || 1;
    candles.forEach(function (c, i) {
      var cx = padL + cw * i + cw / 2, vh = (c.v || 0) / maxV * volH;
      ctx.fillStyle = c.c >= c.o ? up : down; ctx.globalAlpha = 0.5;
      ctx.fillRect(cx - bodyW / 2, volTop + volH - vh, bodyW, vh);
    });
    ctx.globalAlpha = 1;

    // ── 副圖（MACD 柱＋DIF/DEA、KD 雙線、RSI）：金叉死叉、背離要「看得到發生在哪一根」──
    if (hasSub) {
      var vals = [], keys = sub === 'macd' ? ['dif', 'dea', 'osc'] : (sub === 'kd' ? ['k', 'd'] : ['rsi']);
      candles.forEach(function (c) { keys.forEach(function (k2) { if (c[k2] != null) vals.push(c[k2]); }); });
      var shi, slo;
      if (sub === 'macd') {
        var m = Math.max.apply(null, vals.map(Math.abs)) || 1; shi = m; slo = -m;   // MACD 對稱、零軸置中
      } else { shi = 100; slo = 0; }                                                // KD / RSI 固定 0~100
      function sy(v) { return subTop + (shi - v) / (shi - slo) * subH; }
      // 參考線：MACD 零軸；KD 20/80；RSI 30/70
      var refs = sub === 'macd' ? [0] : (sub === 'kd' ? [20, 50, 80] : [30, 50, 70]);
      ctx.font = '10px -apple-system,system-ui,sans-serif'; ctx.textBaseline = 'middle';
      refs.forEach(function (r) {
        var ry = sy(r);
        ctx.strokeStyle = 'rgba(125,141,163,.18)'; ctx.lineWidth = 1;
        ctx.beginPath(); ctx.moveTo(padL, ry); ctx.lineTo(W - padR, ry); ctx.stroke();
        ctx.fillStyle = '#7d8da3'; ctx.fillText(String(r), 4, ry);
      });
      if (sub === 'macd') {
        candles.forEach(function (c, i2) {
          if (c.osc == null) return;
          var cx2 = padL + cw * i2 + cw / 2, y0 = sy(0), y1 = sy(c.osc);
          ctx.fillStyle = c.osc >= 0 ? up : down; ctx.globalAlpha = .75;
          ctx.fillRect(cx2 - bodyW / 2, Math.min(y0, y1), bodyW, Math.max(1, Math.abs(y1 - y0)));
        });
        ctx.globalAlpha = 1;
      }
      function subLine(key, color) {
        ctx.strokeStyle = color; ctx.lineWidth = 1.2; ctx.beginPath(); var st2 = false;
        candles.forEach(function (c, i2) {
          if (c[key] == null) return;
          var cx2 = padL + cw * i2 + cw / 2, yy2 = sy(c[key]);
          st2 ? ctx.lineTo(cx2, yy2) : (ctx.moveTo(cx2, yy2), st2 = true);
        });
        ctx.stroke();
      }
      if (sub === 'macd') { subLine('dif', '#f0b429'); subLine('dea', '#3d8ef8'); }
      else if (sub === 'kd') { subLine('k', '#f0b429'); subLine('d', '#3d8ef8'); }
      else { subLine('rsi', '#b06fe0'); }
    }

    // 十字游標（指到哪根畫哪根）：垂直線貫穿價量與副圖，水平線對齊該根收盤價並標價
    if (hoverIdx != null && candles[hoverIdx]) {
      var hc = candles[hoverIdx], hx = padL + cw * hoverIdx + cw / 2, hy = y(hc.c);
      ctx.save();
      ctx.strokeStyle = 'rgba(180,200,225,.55)'; ctx.lineWidth = 1; ctx.setLineDash([3, 3]);
      ctx.beginPath(); ctx.moveTo(hx, padT); ctx.lineTo(hx, hasSub ? subTop + subH : volTop + volH); ctx.stroke();
      ctx.beginPath(); ctx.moveTo(padL, hy); ctx.lineTo(W - padR, hy); ctx.stroke();
      ctx.setLineDash([]);
      var lbl = hc.c.toFixed(1);
      ctx.fillStyle = 'rgba(20,30,44,.95)'; ctx.fillRect(0, hy - 8, padL - 4, 16);
      ctx.fillStyle = '#e8eef6'; ctx.fillText(lbl, 4, hy);
      ctx.restore();
    }
  }

  function renderTwTech(d) {
    var h = '';
    var mf = d.mf_score || {};
    if (mf.breakdown && mf.breakdown.length) {
      h += '<div class="sd-sec">綜合評分明細（' + num(mf.total) + '/' + num(mf.max) + '）</div>';
      h += mf.breakdown.map(function (f) {
        return '<div class="sd-fac"><span class="dot ' + (f.pass ? 'ok' : 'no') + '"></span>'
          + '<span class="fl">' + esc(f.name || f.label || '') + '</span><span class="fd">' + esc(f.detail || '') + '</span></div>';
      }).join('');
    }
    h += '<div class="sd-sec">均線 / 月線扣抵</div><div class="sd-grid">'
      + kv('現價', num(d.price)) + kv('20MA（月線）', num(d.ma20))
      + kv('5MA', num(d.ma5)) + kv('60MA（季線）', num(d.ma60))
      + kv('月線方向', '<small>' + esc(d.ma20_trend || '—') + '</small>')
      + kv('乖離率(20MA)', num(d.bias20) + '%') + '</div>';
    h += '<div class="sd-sec">技術指標</div><div class="sd-grid">'
      + kv('KD', 'K ' + num(d.k) + ' / D ' + num(d.d) + '<br><small>' + esc(d.kd_status || '') + '</small>')
      + kv('MACD 柱', num(d.osc) + '<br><small>' + esc(d.osc_trend || '') + '</small>')
      + kv('RSI', num(d.rsi)) + kv('量能結構', '<small>' + esc(d.vol_structure || '—') + '</small>')
      + '</div>';
    h += '<div class="sd-sec">位階 / 支撐壓力</div><div class="sd-grid">'
      + kv('52週位階', num(d.w52_pct) + '%') + kv('距低點反彈', num(d.rebound_pct) + '%')
      + kv('52週高 / 低', num(d.w52_high) + ' / ' + num(d.w52_low))
      + kv('近20日高 / 低', num(d.high_20d) + ' / ' + num(d.low_20d)) + '</div>';
    if (d.is_etf && d.etf) {
      var e = d.etf;
      h += '<div class="sd-sec">ETF 資訊</div><div class="sd-grid">'
        + kv('殖利率', num(e.yield) + '%') + kv('淨值', num(e.nav))
        + kv('溢價/折價', num(e.premium_pct) + '%') + kv('費用率', num(e.expense) + '%') + '</div>';
    }
    elBody.innerHTML = h;
  }

  function netCls(v) { return v > 0 ? 'sd-pos' : (v < 0 ? 'sd-neg' : ''); }
  function signed(v) { v = +v || 0; return (v > 0 ? '+' : '') + v.toLocaleString(); }
  function lots(v) { return Math.round((+v || 0) / 1000); }   // FinMind 法人為「股」，換算成「張」

  function renderTwChips(d) {
    var h = '';
    var inst = d.inst, mg = d.margin;
    if (inst) {
      h += '<div class="sd-sec">三大法人（張）</div><div class="sd-grid">'
        + kv('外資', signed(lots(inst.foreign_net)), netCls(inst.foreign_net))
        + kv('投信', signed(lots(inst.trust_net)), netCls(inst.trust_net))
        + kv('自營商', signed(lots(inst.dealer_net)), netCls(inst.dealer_net))
        + kv('合計', signed(lots(inst.total_net)), netCls(inst.total_net)) + '</div>';
    } else { h += '<div class="sd-sec">三大法人</div><div class="sd-empty">尚無法人資料（非交易時段或未取得）</div>'; }
    if (mg) {
      h += '<div class="sd-sec">融資融券</div><div class="sd-grid">'
        + kv('融資餘額', num(mg.margin_today)) + kv('融資增減', signed(mg.margin_chg), netCls(-mg.margin_chg))
        + kv('融券餘額', num(mg.short_today)) + kv('融券增減', signed(mg.short_chg)) + '</div>';
    }
    var extra = '';
    if (d.lending) extra += kv('借券賣出餘額', num(d.lending.lending_balance));
    if (d.day_trade_ratio != null) extra += kv('當沖比', num(d.day_trade_ratio) + '%');
    if (extra) h += '<div class="sd-sec">其他籌碼</div><div class="sd-grid">' + extra + '</div>';
    if (!h) h = '<div class="sd-empty">此標的暫無籌碼資料</div>';
    h += '<div class="sd-sec">逐日籌碼（近 10 交易日）</div><div id="sd-chiphist">'
      + (state.chips ? chipHistHtml(state.chips) : '<div class="sd-loading">籌碼歷史載入中…</div>') + '</div>';
    elBody.innerHTML = h;
    if (!state.chips) loadChipHist();
  }

  // 逐日籌碼（法人買賣超／資券／借券）；純資料查詢、不耗 token，只在點開籌碼分頁時才抓
  function loadChipHist() {
    var code = state.code;
    getJson('/api/predict/chips/' + code + '?days=10')
      .catch(function () { return { error: '籌碼歷史載入失敗' }; })
      .then(function (r) {
        if (state.code !== code) return;            // 使用者已切換到別檔
        state.chips = r;
        var box = document.getElementById('sd-chiphist');
        if (box) box.innerHTML = chipHistHtml(r);
      });
  }

  function chipHistHtml(r) {
    var rows = (r && r.rows) || [];
    if (!rows.length) return '<div class="sd-empty">' + esc((r && r.error) || '暫無逐日籌碼資料') + '</div>';
    function td(v, invert) {
      if (v === null || v === undefined) return '<td>—</td>';
      var cls = invert ? netCls(-v) : netCls(v);
      return '<td class="' + cls + '">' + signed(v) + '</td>';
    }
    var h = '<div class="sd-tblwrap"><table class="sd-tbl"><thead><tr>'
      + '<th>日期</th><th>外資</th><th>投信</th><th>自營</th><th>合計</th>'
      + '<th>融資增減</th><th>融券增減</th></tr></thead><tbody>';
    rows.forEach(function (x) {
      h += '<tr><td>' + esc((x.d || '').slice(5)) + '</td>'
        + td(x.foreign == null ? null : lots(x.foreign))
        + td(x.trust   == null ? null : lots(x.trust))
        + td(x.dealer  == null ? null : lots(x.dealer))
        + td(x.total   == null ? null : lots(x.total))
        + td(x.margin_chg, true)      // 融資增加＝散戶追價，視為偏空，故反色
        + td(x.short_chg)
        + '</tr>';
    });
    [['sum5', '近5日累計'], ['sum10', '近10日累計']].forEach(function (p) {
      var s = r[p[0]]; if (!s) return;
      h += '<tr class="sum"><th>' + p[1] + '</th>'
        + td(lots(s.foreign)) + td(lots(s.trust)) + td(lots(s.dealer)) + td(lots(s.total))
        + '<td colspan="2">買超 ' + s.up + '/' + s.days + ' 日</td></tr>';
    });
    h += '</tbody></table></div>'
      + '<div class="sd-capnote" style="font-size:.72rem;color:var(--text-dim,#7d8da3);margin-top:6px;">'
      + '法人單位：張（＝股數/1000）；融資融券單位：張。融資增減以「增加」為偏空著色。</div>';
    return h;
  }

  // ════════════ 美股 ════════════
  function loadUs(code) {
    Promise.all([
      getJson('/api/fundamentals/' + code).catch(function () { return {}; }),
      getJson('/api/news/' + code).catch(function () { return { articles: [] }; }),
      getJson('/api/predict/kline/' + code + '?tw=0&period=3mo&interval=1d').catch(function () { return { error: 'K線載入失敗' }; })
    ]).then(function (arr) {
      state.data = arr[0]; state.news = (arr[1] && arr[1].articles) || []; state.kline = arr[2];
      renderUsHead(); renderUsTabs(); renderUsFoot(); renderTab();
    });
  }

  function renderUsHead() {
    var d = state.data || {};
    elHead.innerHTML = '<div class="sd-titlewrap"><div class="sd-code">' + esc(state.code) + '</div>'
      + '<div class="sd-name">US Stock · Fundamentals</div></div>'
      + '<button class="sd-close" title="關閉">&times;</button>';
    elHead.querySelector('.sd-close').onclick = closeDetail;
  }
  function renderUsTabs() {
    renderTabsBar([['kline', 'K線'], ['intraday', '分時'], ['fund', '基本面'], ['news', '新聞']], renderUsTabs);
  }
  function renderUsFund() {
    var d = state.data || {};
    if (d.error) { elBody.innerHTML = '<div class="sd-empty">' + esc(d.error) + '</div>'; return; }
    var h = '<div class="sd-sec">現金流 / 估值</div><div class="sd-grid">'
      + kv('自由現金流', num(d.fcf) + '<small> B</small>') + kv('FCF 殖利率', num(d.fcfYield) + '%')
      + kv('P/FCF', num(d.pfcf)) + kv('營業現金流', num(d.ocf) + '<small> B</small>') + '</div>';
    h += '<div class="sd-sec">獲利能力</div><div class="sd-grid">'
      + kv('ROE', num(d.roe) + '%') + kv('ROA', num(d.roa) + '%')
      + kv('毛利率', num(d.grossMargin) + '%') + kv('淨利率', num(d.profitMargin) + '%') + '</div>';
    h += '<div class="sd-sec">財務 / 成長 / 籌碼</div><div class="sd-grid">'
      + kv('負債權益比', num(d.debtEquity)) + kv('流動比', num(d.currentRatio))
      + kv('營收成長', num(d.revGrowth) + '%') + kv('EPS 成長', num(d.epsGrowth) + '%')
      + kv('法人持股', num(d.instPct) + '%') + kv('內部人持股', num(d.insiderPct) + '%')
      + kv('放空比', num(d.shortPct) + '%') + kv('財報日', '<small>' + esc(d.earningsDate || '—') + '</small>') + '</div>';
    if (d.topHolders && d.topHolders.length) {
      h += '<div class="sd-sec">主要法人股東</div>';
      h += d.topHolders.map(function (x) {
        return '<div class="sd-fac"><span class="fl">' + esc(x.holder) + '</span><span class="fd">'
          + num(x.pct) + '%</span></div>'; }).join('');
    }
    elBody.innerHTML = h;
  }

  // ════════════ 共用：新聞 / footer ════════════
  function renderNews() {
    var arr = state.news || [];
    if (!arr.length) { elBody.innerHTML = '<div class="sd-empty">暫無相關新聞</div>'; return; }
    elBody.innerHTML = arr.map(function (a) {
      return '<a class="sd-news" href="' + esc(a.url) + '" target="_blank" rel="noopener">'
        + '<div class="nt">' + esc(a.title) + '</div>'
        + '<div class="nm">' + esc(a.publisher || '') + (a.pubTime ? ' · ' + esc(String(a.pubTime).slice(0, 10)) : '') + '</div></a>';
    }).join('');
  }

  function renderTwFoot() {
    elFoot.innerHTML = '<button class="sd-btn primary" id="sdMon">加入監測</button>'
      + '<a class="sd-btn" href="/predict?code=' + encodeURIComponent(state.code) + '" target="_blank">AI 明日預測</a>'
      + '<a class="sd-btn" href="/tw?ticker=' + encodeURIComponent(state.code) + '" target="_blank">完整分析</a>';
    elFoot.querySelector('#sdMon').onclick = addMonitor;
  }
  function renderUsFoot() {
    elFoot.innerHTML = '<a class="sd-btn primary" href="/?ticker=' + encodeURIComponent(state.code) + '" target="_blank">完整分析頁</a>';
  }

  function addMonitor() {
    var btn = elFoot.querySelector('#sdMon'); if (!btn) return;
    btn.textContent = '加入中…'; btn.disabled = true;
    var hasLine = false;
    getJson('/api/tw/line/config').catch(function () { return {}; })
      .then(function (lc) {
        hasLine = !!(lc.line_token && lc.line_user_id);
        return fetch('/api/tw/monitor/register', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ticker: state.code, profile: 'perpetual',
            line_token: lc.line_token || '', line_user_id: lc.line_user_id || '' })
        });
      }).then(function (r) { return r.json(); })
      .then(function () { btn.textContent = hasLine ? '已加入監測' : '已加入(未設LINE)'; })
      .catch(function () { btn.textContent = '加入失敗'; btn.disabled = false; });
  }

  // 全站委派：任何帶 data-sd-code 的元素點了就開面板。
  // 用 capture 階段並 stopPropagation，避免同時觸發父層既有的 onclick。
  document.addEventListener('click', function (e) {
    var el = e.target && e.target.closest ? e.target.closest('[data-sd-code]') : null;
    if (!el) return;
    e.preventDefault(); e.stopPropagation();
    var c = el.getAttribute('data-sd-code');
    var tw = el.getAttribute('data-sd-tw');
    window.openStockDetail(c, tw == null ? undefined : (tw === '1' || tw === 'true'));
  }, true);
})();

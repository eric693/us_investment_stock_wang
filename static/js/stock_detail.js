/* ============================================================
 * 全站共用：個股詳情滑出面板（drawer）
 * 用法：openStockDetail(code, isTw)
 *   - 台股：整合 技術/起漲評分 + 籌碼(法人/融資/借券/當沖) + 新聞
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
    state = { code: code, isTw: !!isTw, tab: 0, data: null, news: null };
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
      getJson('/api/tw/news/' + code).catch(function () { return { articles: [] }; })
    ]).then(function (arr) {
      state.data = arr[0]; state.news = (arr[1] && arr[1].articles) || [];
      renderTwHead(); renderTwTabs(); renderTwFoot(); renderTab();
    });
  }

  function gradeColor(g) {
    return { A: 'background:rgba(0,214,143,.15);color:#00d68f', B: 'background:rgba(61,142,248,.15);color:#3d8ef8',
      C: 'background:rgba(240,180,41,.15);color:#f0b429', D: 'background:rgba(232,120,70,.15);color:#e87846',
      F: 'background:rgba(232,70,70,.15);color:#e84646' }[g] || 'background:rgba(125,141,163,.15);color:#7d8da3';
  }

  function renderTwHead(d) {
    d = state.data || {};
    var mf = d.mf_score || {};
    var grade = mf.grade ? '<span class="sd-grade" style="' + gradeColor(mf.grade) + '">起漲評分 ' + esc(mf.grade)
      + ' · ' + num(mf.pct, '?') + '%</span>' : '';
    elHead.innerHTML = '<div class="sd-titlewrap"><div class="sd-code">' + esc(d.name || state.code)
      + ' <small style="color:var(--text-dim);font-weight:600">' + esc(state.code) + (d.is_etf ? ' · ETF' : '') + '</small></div>'
      + '<div class="sd-name">台股 · 即時技術與籌碼</div>' + grade + '</div>'
      + '<div><div class="sd-price">' + (d.price != null ? d.price : '—') + '</div></div>'
      + '<button class="sd-close" title="關閉">&times;</button>';
    elHead.querySelector('.sd-close').onclick = closeDetail;
  }

  function renderTwTabs() {
    var tabs = ['技術 / 起漲', '籌碼', '新聞'];
    elTabs.innerHTML = tabs.map(function (t, i) {
      return '<button class="sd-tab' + (i === state.tab ? ' active' : '') + '" data-i="' + i + '">' + t + '</button>';
    }).join('');
    Array.prototype.forEach.call(elTabs.querySelectorAll('.sd-tab'), function (b) {
      b.onclick = function () { state.tab = +b.dataset.i; renderTwTabs(); renderTab(); };
    });
  }

  function kv(k, v, cls) { return '<div class="sd-kv"><div class="sd-k">' + k + '</div><div class="sd-v ' + (cls || '') + '">' + v + '</div></div>'; }

  function renderTab() {
    if (state.isTw) {
      var d = state.data || {};
      if (d.error) { elBody.innerHTML = '<div class="sd-empty">' + esc(d.error) + '</div>'; return; }
      if (state.tab === 0) return renderTwTech(d);
      if (state.tab === 1) return renderTwChips(d);
      return renderNews();
    } else {
      if (state.tab === 0) return renderUsFund();
      return renderNews();
    }
  }

  function renderTwTech(d) {
    var h = '';
    var mf = d.mf_score || {};
    if (mf.breakdown && mf.breakdown.length) {
      h += '<div class="sd-sec">起漲評分明細（' + num(mf.total) + '/' + num(mf.max) + '）</div>';
      h += mf.breakdown.map(function (f) {
        return '<div class="sd-fac"><span class="dot ' + (f.pass ? 'ok' : 'no') + '"></span>'
          + '<span class="fl">' + esc(f.label) + '</span><span class="fd">' + esc(f.detail || '') + '</span></div>';
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

  function renderTwChips(d) {
    var h = '';
    var inst = d.inst, mg = d.margin;
    if (inst) {
      h += '<div class="sd-sec">三大法人（張）</div><div class="sd-grid">'
        + kv('外資', signed(inst.foreign_net), netCls(inst.foreign_net))
        + kv('投信', signed(inst.trust_net), netCls(inst.trust_net))
        + kv('自營商', signed(inst.dealer_net), netCls(inst.dealer_net))
        + kv('合計', signed(inst.total_net), netCls(inst.total_net)) + '</div>';
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
    elBody.innerHTML = h;
  }

  // ════════════ 美股 ════════════
  function loadUs(code) {
    Promise.all([
      getJson('/api/fundamentals/' + code).catch(function () { return {}; }),
      getJson('/api/news/' + code).catch(function () { return { articles: [] }; })
    ]).then(function (arr) {
      state.data = arr[0]; state.news = (arr[1] && arr[1].articles) || [];
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
    var tabs = ['基本面', '新聞'];
    elTabs.innerHTML = tabs.map(function (t, i) {
      return '<button class="sd-tab' + (i === state.tab ? ' active' : '') + '" data-i="' + i + '">' + t + '</button>';
    }).join('');
    Array.prototype.forEach.call(elTabs.querySelectorAll('.sd-tab'), function (b) {
      b.onclick = function () { state.tab = +b.dataset.i; renderUsTabs(); renderTab(); };
    });
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
      + '<a class="sd-btn" href="/tw?ticker=' + encodeURIComponent(state.code) + '" target="_blank">完整分析頁</a>'
      + '<a class="sd-btn" href="/perpetual?code=' + encodeURIComponent(state.code) + '" target="_blank">永動機</a>';
    elFoot.querySelector('#sdMon').onclick = addMonitor;
  }
  function renderUsFoot() {
    elFoot.innerHTML = '<a class="sd-btn primary" href="/?ticker=' + encodeURIComponent(state.code) + '" target="_blank">完整分析頁</a>';
  }

  function addMonitor() {
    var btn = elFoot.querySelector('#sdMon'); if (!btn) return;
    btn.textContent = '加入中…'; btn.disabled = true;
    getJson('/api/tw/line/config').catch(function () { return {}; })
      .then(function (lc) {
        return fetch('/api/tw/monitor/register', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ticker: state.code, profile: 'steady',
            line_token: lc.line_token || '', line_user_id: lc.line_user_id || '' })
        });
      }).then(function (r) { return r.json(); })
      .then(function () { btn.textContent = '已加入監測'; })
      .catch(function () { btn.textContent = '加入失敗'; btn.disabled = false; });
  }
})();

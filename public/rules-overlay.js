/* ============================================================
   شروط اللعب — طبقة بطاقات كوميك بوك مشتركة لكل الأوفرليات
   فعاليات تيك توك — تُحقن في كل *-overlay.html
   تستمع لأحداث rules:show / rules:hide وتعرض البطاقات فوق الأوفرلي
   ============================================================ */
(function () {
  'use strict';

  var params = new URLSearchParams(location.search);
  var username = params.get('username') || params.get('user') || params.get('u') || '';
  if (!username || typeof io === 'undefined') return;

  // اتصال مستقل حتى لا نلمس منطق الأوفرلي الأصلي
  var rSocket = io();
  rSocket.on('connect', function () {
    rSocket.emit('join', { username: username, key: username });
  });

  // خط كوميك (Lalezar) — يُحمَّل عند الحاجة فقط
  var fontLoaded = false;
  function loadFont() {
    if (fontLoaded) return;
    fontLoaded = true;
    var l = document.createElement('link');
    l.rel = 'stylesheet';
    l.href = 'https://fonts.googleapis.com/css2?family=Lalezar&family=Tajawal:wght@700;900&display=swap';
    document.head.appendChild(l);
  }

  // CSS الكوميك
  var css = [
    '#rules-comic-layer{position:fixed;inset:0;z-index:99999;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:4vh 4vw;background:rgba(5,5,12,.55) !important;opacity:0;transition:opacity .3s;pointer-events:none;direction:rtl}',
    '#rules-comic-layer.show{opacity:1}',
    /* أثناء عرض الشروط: إخفاء أي واجهة تسجيل بالأوفرلي */
    'body.rules-active #reg-banner,body.rules-active #reg-panel,body.rules-active .reg-panel,body.rules-active .reg-banner,body.rules-active #join-keywords,body.rules-active #reg-info,body.rules-active #reg-timer{visibility:hidden !important}',
    '#rules-comic-layer .rc-title{font-family:"Lalezar","Tajawal",sans-serif;font-size:clamp(2rem,5.5vw,3.6rem);color:#ffd93d;background:#ff5252;padding:.15em .9em .05em;border:5px solid #000;border-radius:18px;box-shadow:9px 9px 0 #000;transform:rotate(-2.5deg);letter-spacing:1px;margin-bottom:3.5vh;text-shadow:3px 3px 0 #000;animation:rcPop .45s cubic-bezier(.2,1.6,.4,1) both}',
    '#rules-comic-layer .rc-cards{display:flex;flex-direction:column;gap:2.2vh;width:min(92vw,720px)}',
    '#rules-comic-layer .rc-card{display:flex;align-items:center;gap:16px;background:#fff;border:4px solid #000;border-radius:16px;box-shadow:7px 7px 0 #000;padding:12px 18px;font-family:"Tajawal",sans-serif;font-weight:900;font-size:clamp(1rem,2.6vw,1.45rem);color:#111;animation:rcPop .45s cubic-bezier(.2,1.6,.4,1) both}',
    '#rules-comic-layer .rc-card:nth-child(odd){transform:rotate(1.2deg)}',
    '#rules-comic-layer .rc-card:nth-child(even){transform:rotate(-1.2deg)}',
    '#rules-comic-layer .rc-card:nth-child(1){background:#ffd93d}',
    '#rules-comic-layer .rc-card:nth-child(2){background:#4ecdc4}',
    '#rules-comic-layer .rc-card:nth-child(3){background:#ff8fb1}',
    '#rules-comic-layer .rc-card:nth-child(4){background:#a8e6cf}',
    '#rules-comic-layer .rc-card:nth-child(5){background:#c3aeff}',
    '#rules-comic-layer .rc-card:nth-child(6){background:#ffb347}',
    '#rules-comic-layer .rc-card:nth-child(7){background:#87e0ff}',
    '#rules-comic-layer .rc-card:nth-child(8){background:#ffe08a}',
    '#rules-comic-layer .rc-num{flex-shrink:0;width:46px;height:46px;border-radius:50%;background:#111;color:#ffd93d;border:4px solid #000;display:flex;align-items:center;justify-content:center;font-family:"Lalezar",sans-serif;font-size:1.5rem;box-shadow:3px 3px 0 rgba(0,0,0,.35)}',
    '#rules-comic-layer .rc-timer{margin-top:3.5vh;width:min(70vw,420px);height:18px;background:#fff;border:4px solid #000;border-radius:20px;box-shadow:5px 5px 0 #000;overflow:hidden}',
    '#rules-comic-layer .rc-timer i{display:block;height:100%;width:100%;background:repeating-linear-gradient(45deg,#ff5252 0 14px,#ffd93d 14px 28px);transition:width .25s linear}',
    '@keyframes rcPop{0%{opacity:0;transform:scale(.5) rotate(-6deg)}100%{opacity:1}}',
  ].join('\n');

  var styleEl = null, layer = null, hideTimer = null, tickTimer = null;

  function ensureLayer() {
    loadFont();
    if (!styleEl) {
      styleEl = document.createElement('style');
      styleEl.textContent = css;
      document.head.appendChild(styleEl);
    }
    if (!layer) {
      layer = document.createElement('div');
      layer.id = 'rules-comic-layer';
      document.body.appendChild(layer);
    }
    return layer;
  }

  function esc(s) {
    return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  function showRules(data) {
    var el = ensureLayer();
    var rules = (data && data.rules) || [];
    if (!rules.length) return;
    var html = '<div class="rc-title">📜 ' + esc(data.title || 'شروط اللعب') + '</div>';
    html += '<div class="rc-cards">';
    for (var i = 0; i < rules.length; i++) {
      html += '<div class="rc-card" style="animation-delay:' + (0.12 * (i + 1)) + 's">' +
        '<span class="rc-num">' + (i + 1) + '</span><span>' + esc(rules[i]) + '</span></div>';
    }
    html += '</div>';
    var duration = parseInt(data.duration) || 0;
    if (duration > 0) html += '<div class="rc-timer"><i id="rc-timer-bar"></i></div>';
    el.innerHTML = html;
    // إخفاء واجهة التسجيل طول مدة عرض الشروط (تظهر تلقائياً بعد الانتهاء)
    document.body.classList.add('rules-active');
    // إعادة تشغيل الترانزيشن
    el.classList.remove('show');
    void el.offsetWidth;
    el.classList.add('show');

    if (hideTimer) { clearTimeout(hideTimer); hideTimer = null; }
    if (tickTimer) { clearInterval(tickTimer); tickTimer = null; }
    if (duration > 0) {
      var endAt = Date.now() + duration * 1000;
      tickTimer = setInterval(function () {
        var bar = document.getElementById('rc-timer-bar');
        if (!bar) return;
        var remain = Math.max(0, endAt - Date.now());
        bar.style.width = (remain / (duration * 1000) * 100) + '%';
      }, 200);
      hideTimer = setTimeout(hideRules, duration * 1000);
    }
  }

  function hideRules() {
    if (hideTimer) { clearTimeout(hideTimer); hideTimer = null; }
    if (tickTimer) { clearInterval(tickTimer); tickTimer = null; }
    document.body.classList.remove('rules-active');
    if (layer) {
      layer.classList.remove('show');
      setTimeout(function () { if (layer) layer.innerHTML = ''; }, 350);
    }
  }

  rSocket.on('rules:show', showRules);
  rSocket.on('rules:hide', hideRules);
})();

/* ============================================================
   ودجت الشرح والشروط — يُحقن بكل لوحات تحكم الفعاليات
   زر عائم 📜 يفتح لوحة فيها: شرح الفعالية + شروط اللعب القابلة
   للتعديل + زر عرضها بالأوفرلي بستايل الكوميك
   ============================================================ */
(function () {
  'use strict';
  // اللوحات المعاد بناؤها عندها شروط مدمجة — لا نكرر
  if (document.getElementById('rules-text')) return;

  var GAME_META = {
    'wheel': { icon:'🎡', name:'عجلة الحظ', desc:'سحب عشوائي: الجمهور يسجلون بكلمة معينة، أسماؤهم تدخل العجلة، وتلف العجلة لتختار فائزاً واحداً.', rules:['اكتب كلمة التسجيل بالشات عشان تدخل العجلة','كل شخص يدخل مرة وحدة بس','انتظر لين يقفل التسجيل وتلف العجلة','اللي تطلع عليه العجلة هو الفائز 🏆'] },
    'slot-game': { icon:'🎴', name:'البطاقات', desc:'سحب بطاقات: المشتركون يدخلون بكلمة التسجيل وتُسحب بطاقة عشوائية تحدد الفائز.', rules:['اكتب كلمة التسجيل بالشات عشان تدخل السحب','كل شخص يدخل مرة وحدة','انتظر السحب — اللي تطلع بطاقته يفوز 🏆'] },
    'guess-game': { icon:'💡', name:'خمن الرقم', desc:'المضيف يحدد رقماً سرياً ضمن نطاق، والجمهور يخمنون بكتابة الأرقام بالشات حتى يصيبه أحدهم.', rules:['فيه رقم سري بين الحد الأدنى والأعلى المعروضين','اكتب تخمينك رقم بالشات','أول من يصيب الرقم الصحيح يفوز 🏆'] },
    'person-game': { icon:'🎭', name:'خمن الشخصية', desc:'صورة شخصية مشهورة تظهر بالأوفرلي، والجمهور يخمنون اسمها بالشات.', rules:['شوف الصورة والتلميحات المعروضة','اكتب اسم الشخصية بالشات','أول من يكتب الاسم الصحيح يفوز 🏆'] },
    'countdown-game': { icon:'⏰', name:'العد التنازلي', desc:'عد تنازلي يظهر للجمهور، وأول من يكتب الكلمة المطلوبة بعد وصوله للصفر يفوز.', rules:['انتظر انتهاء العد التنازلي','أول من يكتب الكلمة المطلوبة بعد الصفر يفوز 🏆','اللي يكتب قبل الصفر ما يُحتسب'] },
    'mystery-game': { icon:'📦', name:'الصندوق الغامض', desc:'صناديق مرقمة فيها مفاجآت — الجمهور يختارون صندوقاً بكتابة رقمه، والمضيف يفتح الصناديق ويكشف الجوائز.', rules:['اختر رقم صندوق واكتبه بالشات','كل صندوق فيه مفاجأة مخفية','انتظر الفتح وشوف حظك 🎁'] },
    'photo-challenge': { icon:'📸', name:'تحدي الصور', desc:'صورة مقربة أو مموهة تظهر تدريجياً، والجمهور يخمنون ما فيها.', rules:['شوف الصورة المعروضة بتركيز','خمن وش فيها واكتب إجابتك بالشات','أول إجابة صحيحة تفوز 🏆'] },
    'secret-word': { icon:'🔐', name:'كلمة السر', desc:'كلمة سرية يحددها المضيف، والجمهور يجربون كلمات بالشات حتى يكتشفها أحدهم.', rules:['فيه كلمة سرية مخفية','جرب كلمات بالشات','أول من يكتب كلمة السر بالضبط يفوز 🏆'] },
    'sounds': { icon:'🔊', name:'الأصوات', desc:'مقطع صوتي يُشغّل بالبث، والجمهور يخمنون مصدره أو صاحبه.', rules:['اسمع الصوت المشغل زين','خمن مصدره واكتب إجابتك بالشات','أول إجابة صحيحة تفوز 🏆'] },
    'memory-game': { icon:'🧠', name:'تحدي الذاكرة', desc:'عناصر تظهر لثوانٍ ثم تختفي، والجمهور يكتبون ما يتذكرونه منها.', rules:['ركز على العناصر المعروضة قبل ما تختفي','اكتب اللي تتذكره بالشات','صاحب أدق ذاكرة يفوز 🏆'] },
    'million-game': { icon:'💰', name:'من سيربح المليون', desc:'أسئلة متدرجة الصعوبة بأسلوب المليونير — الجمهور يجاوبون بكتابة رقم الإجابة.', rules:['جاوب بكتابة رقم الإجابة من 1 إلى 4','الأسئلة تصعب كل ما تقدمت','اجمع أكبر رصيد وكن البطل 🏆'] },
    'draw-game': { icon:'🖼️', name:'ما هذا الرسم', desc:'رسمة تكتمل تدريجياً على الشاشة، والجمهور يتسابقون على تخمينها.', rules:['شوف الرسم وهو يكتمل قدامك','خمن وش المرسوم واكتب إجابتك','أول إجابة صحيحة تفوز 🏆'] },
    'letter-race': { icon:'🏃', name:'سباق الحروف', desc:'حرف يظهر على الشاشة والجمهور يتسابقون بكتابة كلمات تبدأ به — كل كلمة صحيحة نقطة.', rules:['اكتب كلمة تبدأ بالحرف المعروض','كل كلمة صحيحة = نقطة لك','الأسرع والأكثر نقاط يفوز 🏆'] },
    'auction': { icon:'💬', name:'مزاد الكلمات', desc:'مزاد حي: الجمهور يزايدون بكتابة أرقام أعلى، وأعلى مزايدة عند انتهاء الوقت تفوز.', rules:['زاود بكتابة رقم أعلى من آخر مزايدة','تابع العداد قبل ما يخلص الوقت','أعلى مزايدة عند النهاية تفوز 🏆'] },
    'ice-game': { icon:'🧊', name:'الجليد', desc:'اللاعبون يتجمدون داخل مكعبات جليد، وعليهم كتابة الكلمة المعروضة بسرعة قبل انتهاء الوقت وإلا خرجوا.', rules:['اكتب كلمة التسجيل عشان تدخل اللعبة','لما تتجمد اكتب الكلمة المعروضة بأسرع وقت','اللي ما يذوب قبل نهاية الوقت يطلع — آخر ناجٍ يفوز 🏆'] },
    'horse-race': { icon:'🏁', name:'سباق الخيل', desc:'أربعة فرق بأربعة خيول — كل رسالة من أعضاء الفريق تقدّم حصانهم نحو خط النهاية.', rules:['اختر فريقك بكتابة رقمه من 1 إلى 4','اكتب أي شيء بالشات عشان يتقدم حصان فريقك','أول حصان يوصل خط النهاية يفوز 🏆'] },
    'castle-war': { icon:'🏰', name:'حرب القلاع', desc:'قلعتان متحاربتان — كل رسالة من أعضاء الفريق قذيفة على قلعة الخصم تنقص صحتها.', rules:['اختر قلعتك: اكتب أحمر أو أزرق','كل رسالة منك = هجوم على قلعة الخصم','القلعة اللي تصفّر صحة خصمها تنتصر 🏆'] },
    'roulette': { icon:'☠️', name:'الروليت الروسي', desc:'لعبة حظ قاتلة: المسجلون يدخلون الحلبة، وكل جولة يختار المسدس ضحية عشوائية تخرج — آخر ناجٍ يفوز.', rules:['اكتب كلمة التسجيل عشان تدخل اللعبة','كل جولة المسدس يختار ضحية عشوائية 💀','آخر ناجٍ هو الفائز 🏆'] },
    'knockout-game': { icon:'🏆', name:'بطولة الخروج', desc:'بطولة إقصاء: أسئلة متتالية بأربعة خيارات — من يجاوب غلط أو يتأخر يخرج من البطولة، وآخر لاعب يبقى هو البطل.', rules:['اكتب كلمة التسجيل عشان تنضم للبطولة','جاوب على كل سؤال بكتابة رقم الإجابة من 1 إلى 4','الإجابة الغلط أو التأخير = خروج من البطولة','آخر لاعب يبقى هو البطل 🏆'] },
    'squid-game': { icon:'🦑', name:'لعبة الحبار', desc:'الضوء الأخضر والأحمر: بالأخضر كل رسالة بالشات تقدّمك خطوة نحو خط النهاية، وبالأحمر أي رسالة تقصيك فوراً — أول الواصلين يفوزون.', rules:['اكتب كلمة التسجيل عشان تدخل اللعبة','🟢 الضوء أخضر: اكتب أي شيء — كل رسالة خطوة للأمام','🔴 الضوء أحمر: لا تكتب أبداً — أي رسالة تقصيك 💀','أول من يوصل خط النهاية يفوز 🏆'] },
    'guess-time': { icon:'⏱️', name:'خمن الوقت', desc:'عداد يشتغل ثم يختفي ويتوقف بالخفاء — الجمهور يخمنون عند كم ثانية توقف، والأقرب يفوز.', rules:['راقب العداد قبل ما يختفي','بعد إيقافه اكتب تخمينك بالثواني (مثال: 12.5)','أقرب تخمين للوقت الحقيقي يفوز 🏆'] },
  };

  var page = (location.pathname.split('/').pop() || '').replace('.html', '');
  var meta = GAME_META[page];
  if (!meta) return;

  var params = new URLSearchParams(location.search);
  var username = params.get('username') || params.get('user') || '';

  var css = document.createElement('style');
  css.textContent = [
    '#gm-fab{position:fixed;bottom:16px;left:16px;z-index:9000;width:52px;height:52px;border-radius:50%;border:2px solid #f5c542;background:#0c0c16;color:#f5c542;font-size:1.4rem;cursor:pointer;box-shadow:0 4px 18px rgba(0,0,0,.55),0 0 14px rgba(245,197,66,.25);display:flex;align-items:center;justify-content:center;font-family:inherit;transition:.2s}',
    '#gm-fab:hover{transform:scale(1.08)}',
    '#gm-panel{position:fixed;bottom:78px;left:16px;z-index:9001;width:min(92vw,360px);background:#0c0c16;border:1px solid rgba(245,197,66,.4);border-radius:16px;padding:16px;box-shadow:0 10px 40px rgba(0,0,0,.6);display:none;font-family:"Tajawal",sans-serif;color:#e8eaf2;direction:rtl}',
    '#gm-panel.open{display:block;animation:gmIn .25s ease}',
    '@keyframes gmIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}',
    '#gm-panel h4{font-size:.95rem;font-weight:900;margin:0 0 6px;display:flex;align-items:center;gap:8px}',
    '#gm-panel .gm-desc{font-size:.76rem;color:#9ca3af;line-height:1.8;margin-bottom:12px;background:#10101e;border:1px solid #1c1c2e;border-radius:10px;padding:8px 10px}',
    '#gm-panel label{display:block;font-size:.7rem;color:#8a8fa3;font-weight:700;margin-bottom:4px}',
    '#gm-panel textarea{width:100%;background:#161628;border:1px solid #1c1c2e;color:#e8eaf2;border-radius:10px;padding:8px 10px;font-family:inherit;font-size:.76rem;line-height:1.9;resize:vertical;outline:none;box-sizing:border-box}',
    '#gm-panel textarea:focus{border-color:#22d3ee}',
    '#gm-panel .gm-row{display:flex;gap:6px;margin-top:8px;align-items:center}',
    '#gm-panel input[type=number]{width:64px;background:#161628;border:1px solid #1c1c2e;color:#e8eaf2;border-radius:8px;padding:7px;font-family:inherit;font-size:.76rem;outline:none}',
    '#gm-panel button{font-family:inherit;font-weight:900;border:none;border-radius:10px;cursor:pointer;font-size:.74rem;padding:9px 10px}',
    '#gm-show{flex:1;background:linear-gradient(135deg,#d97706,#f59e0b);color:#fff}',
    '#gm-hide{background:transparent;border:1px solid #1c1c2e;color:#9ca3af}',
    '#gm-status{font-size:.66rem;color:#86efac;margin-top:6px;min-height:1em;text-align:center}',
  ].join('\n');
  document.head.appendChild(css);

  var fab = document.createElement('button');
  fab.id = 'gm-fab';
  fab.title = 'الشرح والشروط';
  fab.textContent = '📜';
  document.body.appendChild(fab);

  var panel = document.createElement('div');
  panel.id = 'gm-panel';
  panel.innerHTML =
    '<h4>' + meta.icon + ' ' + meta.name + '</h4>' +
    '<div class="gm-desc">💡 ' + meta.desc + '</div>' +
    '<label>📜 شروط اللعب — تظهر بالأوفرلي بستايل كوميك (شرط بكل سطر، حتى 8)</label>' +
    '<textarea id="gm-rules" rows="4">' + meta.rules.join('\n') + '</textarea>' +
    '<div class="gm-row">' +
      '<input type="number" id="gm-duration" value="12" min="3" max="120" title="مدة العرض بالثواني">' +
      '<button id="gm-show">📜 اعرض الشروط بالأوفرلي</button>' +
      '<button id="gm-hide">إخفاء</button>' +
    '</div>' +
    '<div id="gm-status"></div>';
  document.body.appendChild(panel);

  fab.addEventListener('click', function () { panel.classList.toggle('open'); });

  function setStatus(t) {
    document.getElementById('gm-status').textContent = t;
    setTimeout(function () { document.getElementById('gm-status').textContent = ''; }, 3000);
  }

  document.getElementById('gm-show').addEventListener('click', function () {
    if (!username) { setStatus('⚠️ ما فيه username بالرابط'); return; }
    var rules = document.getElementById('gm-rules').value.split('\n').map(function (s) { return s.trim(); }).filter(Boolean).slice(0, 8);
    if (!rules.length) return;
    var duration = Math.max(3, Math.min(120, parseInt(document.getElementById('gm-duration').value) || 12));
    fetch('/api/rules/show', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: username, game: page, title: 'شروط ' + meta.name, rules: rules, duration: duration }),
    }).then(function () { setStatus('✅ الشروط معروضة الآن لمدة ' + duration + ' ثانية'); });
  });

  document.getElementById('gm-hide').addEventListener('click', function () {
    if (!username) return;
    fetch('/api/rules/hide', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: username }),
    });
  });
})();

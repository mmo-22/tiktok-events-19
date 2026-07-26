const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const WebSocket = require('ws');
// ── EulerStream (https://www.eulerstream.com) ──────────────
// المفتاح يبقى في السيرفر فقط ولا يوصل للمتصفح أبداً
const EULER_KEY = process.env.EULER_API_KEY || process.env.TIKTOOL_API_KEY; // fallback للتوافق مع الإعداد القديم
const EULER_WS_HOST = process.env.EULER_WS_URL || 'wss://ws.eulerstream.com';
if (!EULER_KEY) console.log('[WARNING] EULER_API_KEY not set! Add it in Railway environment variables.');
const path = require('path');

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: { origin: '*' },
  transports: ['websocket', 'polling'],
  pingInterval: 25000,
  pingTimeout: 20000,
});

app.use(express.json({ limit: '5mb' }));
app.use(express.static(path.join(__dirname, '../public')));

// Image proxy - يجلب صور من مصادر خارجية ويسلمها للمتصفح
// ذلك لتجاوز مشاكل hotlink protection و CORS
app.get('/api/img-proxy', async (req, res) => {
  const url = req.query.url;
  if (!url) return res.status(400).send('missing url');
  try {
    const response = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; TikTokLiveReader/1.0)',
        'Referer': 'https://en.wikipedia.org/',
      },
    });
    if (!response.ok) return res.status(response.status).send('fetch failed');
    const contentType = response.headers.get('content-type') || 'image/jpeg';
    const buffer = Buffer.from(await response.arrayBuffer());
    res.set('Content-Type', contentType);
    res.set('Cache-Control', 'public, max-age=86400');
    res.send(buffer);
  } catch (err) {
    console.error('[img-proxy] error:', err.message);
    res.status(500).send('proxy error');
  }
});

// ── Room store ────────────────────────────────────────────
const rooms = {};
const MAX_STORED = 100;

// Normalize Arabic text for comparison
function normalizeAr(s) {
  if (!s) return '';
  return s.trim()
    .replace(/[أإآا]/g, 'ا')
    .replace(/[ةه]/g, 'ه')
    .replace(/[يى]/g, 'ي')
    .replace(/\u0640/g, '')
    .replace(/[\u064B-\u065F]/g, '')
    .toLowerCase();
}

function normalizePersonAnswer(s) {
  return normalizeAr(s).replace(/\s+/g, '');
}

function broadcast(key, event, data) {
  io.to(`room:${key}`).emit(event, data);
  // Send stats globally for admin page
  if (event === 'stats') io.emit('room:stats', { username: key, stats: data });
}

// ── إعادة اتصال تلقائية منضبطة ──
// سريعة عند الانقطاع الطبيعي، لكن بسقف محاولات + backoff + حد يومي
// حتى لا نكرر أبداً نمط "extreme API abuse"
const RECONNECT_BASE_DELAY = 3000;      // 3 ثواني (توصية الدعم 2-3s)
const RECONNECT_MAX_DELAY = 60000;      // أقصى تأخير دقيقة
const RECONNECT_MAX_ATTEMPTS = 10;      // بعدها يتوقف ويطلب اتصال يدوي
const RECONNECT_DAILY_CAP = 300;        // صمام أمان يومي لكل غرفة

// أكواد إغلاق EulerStream (ClientCloseCode)
const EULER_CLOSE = {
  1000: 'NORMAL',
  1011: 'INTERNAL_SERVER_ERROR',
  4005: 'STREAM_END',
  4006: 'NO_MESSAGES_TIMEOUT',
  4400: 'INVALID_OPTIONS',
  4401: 'INVALID_AUTH',
  4403: 'NO_PERMISSION',
  4404: 'NOT_LIVE',
  4429: 'TOO_MANY_CONNECTIONS',
  4500: 'TIKTOK_CLOSED_CONNECTION',
  4555: 'MAX_LIFETIME_EXCEEDED',
  4556: 'WEBCAST_FETCH_ERROR',
  4557: 'ROOM_INFO_FETCH_ERROR',
};
// أخطاء لا تُصلَح بإعادة المحاولة (مفتاح/صلاحية/إعدادات خاطئة)
const EULER_FATAL = new Set([4400, 4401, 4403]);

function scheduleReconnect(key, reason = '') {
  const room = rooms[key];
  if (!room || room.status === 'removed') return;

  // تصفير العداد اليومي عند تغير اليوم
  const today = new Date().toISOString().slice(0, 10);
  if (room.reconnectDay !== today) { room.reconnectDay = today; room.reconnectDaily = 0; }
  if ((room.reconnectDaily || 0) >= RECONNECT_DAILY_CAP) {
    console.log(`[TikTok] @${key} reached daily reconnect cap (${RECONNECT_DAILY_CAP}) — stopping. Manual reconnect required.`);
    room.status = 'error';
    io.emit('room:status', { username: key, status: 'error' });
    return;
  }

  room.reconnectAttempts = (room.reconnectAttempts || 0) + 1;
  if (room.reconnectAttempts > RECONNECT_MAX_ATTEMPTS) {
    console.log(`[TikTok] @${key} exceeded ${RECONNECT_MAX_ATTEMPTS} consecutive attempts — stopping. Manual reconnect required.`);
    room.status = 'error';
    io.emit('room:status', { username: key, status: 'error' });
    return;
  }

  const delay = Math.min(RECONNECT_BASE_DELAY * Math.pow(2, room.reconnectAttempts - 1), RECONNECT_MAX_DELAY);
  room.reconnectDaily = (room.reconnectDaily || 0) + 1;
  room.status = 'retrying';
  io.emit('room:status', { username: key, status: 'retrying' });
  console.log(`[TikTok] @${key} reconnect ${room.reconnectAttempts}/${RECONNECT_MAX_ATTEMPTS} in ${Math.round(delay/1000)}s${reason ? ' (' + reason + ')' : ''} [today: ${room.reconnectDaily}/${RECONNECT_DAILY_CAP}]`);

  if (room.retryTimer) clearTimeout(room.retryTimer);
  room.retryTimer = setTimeout(() => connectRoom(key, null, { internal: true }), delay);
}

async function connectRoom(username, sessionid = null, opts = {}) {
  const key = username.toLowerCase().replace('@', '').trim();

  // 🛡️ حارس: منع محاولات الاتصال اليدوية المتقاربة
  // (إعادة الاتصال الداخلية المنضبطة لها إيقاعها الخاص فتتجاوز الحارس)
  const now = Date.now();
  if (!opts.internal && rooms[key] && rooms[key].lastConnectAttempt && now - rooms[key].lastConnectAttempt < 30000) {
    const waitSec = Math.ceil((30000 - (now - rooms[key].lastConnectAttempt)) / 1000);
    console.log(`[TikTok] @${key} connect throttled — wait ${waitSec}s`);
    return;
  }

  // الاتصال اليدوي يصفّر عداد المحاولات
  if (!opts.internal && rooms[key]) rooms[key].reconnectAttempts = 0;

  if (!rooms[key]) {
    rooms[key] = {
      tiktok: null,
      stats: { viewers: 0, likes: 0, diamonds: 0, shares: 0, followers: 0 },
      followerSet: new Set(), // لمنع تكرار الإضافة (userId)
      messages: [],
      status: 'idle',
      retryTimer: null,
      sessionid: sessionid || null,
      gifts: {},
    };
  } else if (sessionid) {
    rooms[key].sessionid = sessionid;
  }

  const room = rooms[key];
  if (room.status === 'connected') return;
  room.lastConnectAttempt = Date.now();
  if (room.retryTimer) { clearTimeout(room.retryTimer); room.retryTimer = null; }
  if (room.pingTimer) { clearInterval(room.pingTimer); room.pingTimer = null; }
  // Close old WebSocket properly
  if (room.tiktok) {
    try { room.tiktok.close(); } catch (_) {}
    try { room.tiktok.terminate(); } catch (_) {}
    room.tiktok = null;
  }

  room.status = 'connecting';
  io.emit('room:status', { username: key, status: 'connecting' });
  console.log(`[EulerStream] Connecting to @${key}...`);

  const EventEmitter = require('events');
  const tiktok = new EventEmitter();

  // Connect via EulerStream WebSocket
  // الاتصال من السيرفر مباشرة بالـ apiKey (ما يحتاج JWT — الـ JWT مخصص لتطبيقات المتصفح)
  const wsParams = new URLSearchParams({
    uniqueId: key,
    apiKey: EULER_KEY || '',
    'features.bundleEvents': 'true',      // يجمّع الأحداث داخل packet.messages
    'features.normalizeUniqueId': 'true',
    'features.syntheticPresence': 'true', // أحداث دخول/خروج المشاهدين
  });
  const wsUrl = `${EULER_WS_HOST}?${wsParams.toString()}`;

  const ws = new WebSocket(wsUrl);
  room.tiktok = ws;

  // Helper: normalize user data from EulerStream format
  function u(data) {
    const user = data?.user || data || {};
    return {
      nickname: user.nickname || user.uniqueId || data.nickname || 'unknown',
      uniqueId: user.uniqueId || data.uniqueId || '',
      userId: user.userId || data.userId || user.uniqueId || '',
      profilePictureUrl: user.profilePicture?.urls?.[0] || user.profilePicture?.url?.[0] || user.profilePictureUrl || data.profilePictureUrl || null,
      isModerator: user.isModerator || data.isModerator || false,
      isSubscriber: user.isSubscriber || user.isSubscribe || data.isSubscriber || false,
      followRole: user.followRole ?? user.followInfo?.followStatus ?? data.followRole ?? 0,
    };
  }

  ws.on('open', () => {
    room.status = 'connected';
    room.retryCount = 0;
    room.lastOpenAt = Date.now();
    console.log(`[EulerStream] Connected @${key}`);
    io.emit('room:status', { username: key, status: 'connected', viewers: 0 });
    broadcast(key, 'stats', room.stats);
    
    // Keep-alive ping every 15 seconds
    room.pingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.ping();
      }
    }, 15000);
  });

  ws.on('pong', () => {
    // Connection is alive
  });

  ws.on('close', (code, reason) => {
    if (room.pingTimer) { clearInterval(room.pingTimer); room.pingTimer = null; }
    if (room.status === 'removed') return;

    // اتصال صمد أكثر من دقيقة = مستقر → نصفّر عداد المحاولات المتتالية
    if (room.lastOpenAt && Date.now() - room.lastOpenAt > 60000) {
      room.reconnectAttempts = 0;
    }

    const codeName = EULER_CLOSE[code] || 'UNKNOWN';

    // أخطاء نهائية: المفتاح خاطئ أو الخطة ما تسمح — إعادة المحاولة ما تنفع
    if (EULER_FATAL.has(code)) {
      console.log(`[EulerStream] @${key} fatal close ${code} (${codeName}): ${reason || 'no reason'} — stopping.`);
      room.status = 'error';
      io.emit('room:status', { username: key, status: 'error', error: codeName });
      return;
    }

    if (code === 4429) {
      // Rate limit: محاولة وحدة متأخرة فقط ثم إيقاف
      console.log(`[EulerStream] @${key} too many connections (4429)`);
      if ((room.reconnectAttempts || 0) >= 1) {
        room.status = 'error';
        io.emit('room:status', { username: key, status: 'error' });
        console.log(`[TikTok] @${key} 4429 persisted — stopping. Manual reconnect required.`);
        return;
      }
      room.reconnectAttempts = (room.reconnectAttempts || 0) + 1;
      room.status = 'retrying';
      io.emit('room:status', { username: key, status: 'retrying' });
      if (room.retryTimer) clearTimeout(room.retryTimer);
      room.retryTimer = setTimeout(() => connectRoom(key, null, { internal: true }), 600000);
      return;
    }

    console.log(`[EulerStream] Disconnected @${key} (code: ${code} ${codeName}) — auto-reconnect (controlled)`);
    room.status = 'disconnected';
    io.emit('room:status', { username: key, status: 'disconnected' });
    scheduleReconnect(key, `close ${code}`);
  });

  ws.on('error', (err) => {
    console.log(`[EulerStream] WS Error @${key}: ${err?.message || 'unknown'}`);
    // فشل المصافحة (مثل 522): لو ما تبعه close خلال 5 ثوانٍ نجدول إعادة الاتصال بأنفسنا
    const wsRef = ws;
    setTimeout(() => {
      if (room.tiktok === wsRef && room.status !== 'connected' && room.status !== 'removed' && !room.retryTimer) {
        scheduleReconnect(key, 'ws error without close');
      }
    }, 5000);
  });

  // Map raw TikTok event names to simple names
  const EVENT_MAP = {
    'WebcastChatMessage': 'chat', 'WebcastGiftMessage': 'gift',
    'WebcastLikeMessage': 'like', 'WebcastMemberMessage': 'member',
    'WebcastSocialMessage': 'social', 'WebcastRoomUserSeqMessage': 'roomUserSeq',
    'WebcastEmoteChatMessage': 'emote', 'WebcastLinkMicBattle': 'linkMicBattle',
    'chat': 'chat', 'gift': 'gift', 'like': 'like', 'member': 'member',
    'social': 'social', 'roomUserSeq': 'roomUserSeq', 'roomInfo': 'roomInfo',
    'follow': 'follow', 'share': 'share', 'streamEnd': 'streamEnd',
  };

  const seenEvents = new Set();
  ws.on('message', (raw) => {
    try {
      const str = raw.toString();

      // IMMEDIATE ping/pong - before any parsing
      if (str.includes('"ping"')) {
        ws.send(JSON.stringify({ event: 'pong' }));
      }

      // Debug: log first 3 raw messages for troubleshooting
      if (!room._rawDebug) room._rawDebug = 0;
      if (room._rawDebug < 3) {
        room._rawDebug++;
        console.log(`[RAW#${room._rawDebug}] @${key}: ${str.slice(0, 200)}`);
      }

      const packet = JSON.parse(str);
      const messages = packet.messages || [packet];

      for (const msg of messages) {
        const rawType = msg.event || msg.type;
        if (rawType === 'ping') continue;
        // عداد تشخيصي لكل نوع حدث
        if (!room.eventCounts) room.eventCounts = {};
        room.eventCounts[rawType] = (room.eventCounts[rawType] || 0) + 1;
        const mappedEvent = EVENT_MAP[rawType] || rawType;

        if (mappedEvent && !seenEvents.has(rawType)) {
          seenEvents.add(rawType);
          console.log(`[Event] @${key} "${rawType}" → "${mappedEvent}"`);
        }

        if (mappedEvent) {
          tiktok.emit(mappedEvent, msg.data || msg);
        }
      }
    } catch(e) {}
  });

  tiktok.on('chat', (data) => {
    const usr = u(data);
    const chatMsg = {
      type: 'chat', id: data.msgId || Date.now(),
      user: usr.nickname, uniqueId: usr.uniqueId,
      avatar: usr.profilePictureUrl,
      comment: data.comment || '',
      isModerator: usr.isModerator, isSubscriber: usr.isSubscriber,
      followRole: usr.followRole, ts: Date.now(),
    };
    storeMsg(key, chatMsg);
    broadcast(key, 'chat', chatMsg);

    // Normalize for game handlers
    data.nickname = usr.nickname;
    data.uniqueId = usr.uniqueId;
    data.userId = usr.userId;
    data.profilePictureUrl = usr.profilePictureUrl;
    data.isModerator = usr.isModerator;
    data.isSubscriber = usr.isSubscriber;
    data.followRole = usr.followRole;
    data.comment = data.comment || '';

    // Check wheel keyword
    const wheel = getWheel(key);
    if (wheel.keyword && data.comment && data.comment.trim().includes(wheel.keyword)) {
      console.log(`[Wheel] ${data.nickname}: "${data.comment}" | accepting=${wheel.accepting} | keyword="${wheel.keyword}" | exists=${wheel.entries.has(data.userId)} | removed=${wheel.removedIds.has(data.userId)}`);
    }
    if (wheel.accepting && wheel.keyword && data.comment &&
        data.comment.trim().includes(wheel.keyword) &&
        !wheel.entries.has(data.userId)) {
      const entry = {
        userId: data.userId,
        name: data.nickname || data.uniqueId,
        avatar: data.profilePictureUrl || null,
      };
      wheel.entries.set(data.userId, entry);
      broadcast(key, 'wheel:update', {
        entries: Array.from(wheel.entries.values()),
        count: wheel.entries.size,
        newEntry: entry,
      });
    }

    // Check guess game
    const guessGame = getGuessGame(key);
    const commentClean = normalizeAr(data.comment);
    const wordClean = normalizeAr(guessGame.word);
    if (guessGame.active && commentClean) {
      console.log(`[Guess] Comment: "${commentClean}" | Word: "${wordClean}" | Match: ${commentClean === wordClean}`);
    }
    const alreadyWon = guessGame.winners.some(w => w.userId === data.userId || w.name === (data.nickname || data.uniqueId));
    if (guessGame.active && !guessGame.transitioning &&
        guessGame.winners.length < 5 &&
        commentClean && wordClean &&
        commentClean === wordClean && !alreadyWon) {
      // Update player stats
      const uid = data.userId || data.uniqueId;
      const existing = guessGame.playerStats.get(uid) || { name: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null, totalWords: 0 };
      existing.totalWords += 1;
      existing.name = data.nickname || data.uniqueId;
      existing.avatar = data.profilePictureUrl || null;
      guessGame.playerStats.set(uid, existing);

      const winner = {
        userId: uid,
        name: existing.name,
        avatar: existing.avatar,
        rank: guessGame.winners.length + 1,
        word: guessGame.word,
        totalWords: existing.totalWords,
      };
      guessGame.winners.push(winner);
      console.log(`[Guess] Winner #${winner.rank}: ${winner.name} (total: ${winner.totalWords})`);
      const allPlayers = Array.from(guessGame.playerStats.entries())
        .map(([uid, s]) => ({ userId: uid, name: s.name, avatar: s.avatar, totalWords: s.totalWords }))
        .sort((a,b) => b.totalWords - a.totalWords);
      broadcast(key, 'guess:won', { winner, word: guessGame.word, winners: guessGame.winners, allPlayers });
      if (guessGame.winners.length === 5 && !guessGame.transitioning) {
        guessGame.active = false;
        guessGame.transitioning = true;
        broadcast(key, 'guess:reveal', { word: guessGame.word });
        // لا انتقال تلقائي — ينتظر المستخدم يضغط "التالي"
      }
    }

    // Check Photo Challenge game (تحدي الصور)
    const pcGame = getPhotoChallengeGame(key);

    // Check Secret Word game (كلمة السر)
    const swGame = getSecretWordGame(key);

    // Check Memory Challenge (تحدي الذاكرة)
    const memGame = getMemoryGame(key);

    // Check Millionaire Game (من سيربح المليون)
    const milGame = getMillionaireGame(key);

    // Check Knockout Tournament (بطولة خروج)
    const koGame = getKnockoutGame(key);

    // Check Drawing Game (ما هذا الرسم؟)
    const drawGame = getDrawGame(key);

    // Check Letter Race (سباق الحروف)
    const lrGame = getLetterRace(key);

    // Check Ice Game (الجليد)
    const iceGame = getIceGame(key);

    // Check Horse Race (سباق الخيل)
    const hrGame = getHorseRace(key);

    // Check Squid Game (لعبة الحبار — الضوء الأخضر والأحمر)
    const sqGame = getSquid(key);
    if (sqGame.active && data.comment) {
      const uid = data.userId || data.uniqueId;
      if (sqGame.phase === 'register' && !sqGame.players.has(uid)) {
        const normS = s => String(s || '').trim().toLowerCase().replace(/\s+/g, '').replace(/[أإآ]/g, 'ا');
        if (normS(data.comment).includes(normS(sqGame.keyword)) && sqGame.players.size < sqGame.maxPlayers) {
          sqGame.players.set(uid, { name: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null, steps: 0, alive: true, finished: false });
          broadcast(key, 'squid:joined', {
            player: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null,
            count: sqGame.players.size, max: sqGame.maxPlayers,
          });
        }
      } else if (sqGame.phase === 'playing' && sqGame.players.has(uid)) {
        const p = sqGame.players.get(uid);
        if (p.alive && !p.finished) {
          if (sqGame.light === 'green') {
            p.steps++;
            if (p.steps >= sqGame.finishSteps) {
              p.finished = true;
              sqGame.winners.push({ name: p.name, avatar: p.avatar });
              broadcast(key, 'squid:finished', { player: p.name, avatar: p.avatar, rank: sqGame.winners.length });
              if (sqGame.winners.length >= sqGame.winnersCount || squidAliveCount(sqGame) === 0) squidEnd(key);
            } else {
              broadcast(key, 'squid:step', { player: p.name, avatar: p.avatar, steps: p.steps, finish: sqGame.finishSteps });
            }
          } else if (Date.now() - sqGame.lightChangedAt > 700) {
            // 🔴 كتب أثناء الأحمر (بعد مهلة السماح) → إقصاء
            p.alive = false;
            sqGame.eliminated.push({ name: p.name, avatar: p.avatar });
            broadcast(key, 'squid:eliminated', { player: p.name, avatar: p.avatar, aliveCount: squidAliveCount(sqGame) });
            if (squidAliveCount(sqGame) === 0) squidEnd(key);
          }
        }
      }
    }

    // Check Castle War (حرب القلاع)
    const cwGame = getCastleGame(key);

    // Check Roulette (الروليت الروسي)
    const rlGame = getRoulette(key);

    // Check Guess Time (خمن الوقت)
    const gtGame = getGuessTime(key);
    if (gtGame.state === 'guessing' && data.comment) {
      const uid = data.userId || data.uniqueId;
      // Parse number from comment (Arabic or Western numerals + الفاصلة العشرية العربية)
      const numStr = data.comment.trim()
        .replace(/[٠-٩]/g, d => '٠١٢٣٤٥٦٧٨٩'.indexOf(d))
        .replace(/[٫,]/g, '.');
      const guess = parseFloat(numStr);
      if (!isNaN(guess) && guess > 0 && guess < 999 && !gtGame.guesses.has(uid)) {
        const diff = Math.abs(guess - gtGame.stoppedAt / 1000);
        const entry = { userId: uid, name: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null, guess, diff };
        gtGame.guesses.set(uid, entry);
        // Find current leading (closest guess)
        const stoppedSec = gtGame.stoppedAt / 1000;
        let leading = entry;
        for (const g of gtGame.guesses.values()) {
          if (Math.abs(g.guess - stoppedSec) < Math.abs(leading.guess - stoppedSec)) leading = g;
        }
        broadcast(key, 'gtime:guess', { latest: entry, guessCount: gtGame.guesses.size, leading });
      }
    }
    if (rlGame.active && rlGame.phase === 'register' && data.comment) {
      // مطابقة مرنة: توحيد الهمزات + احتواء بدل التطابق الحرفي
      const norm = s => String(s || '').trim().toLowerCase().replace(/\s+/g, '').replace(/[أإآ]/g, 'ا').replace(/ة/g, 'ه').replace(/ى/g, 'ي');
      const comment = norm(data.comment);
      const kw = norm(rlGame.keyword);
      const uid = data.userId || data.uniqueId;
      if (kw && comment.includes(kw) && !rlGame.players.has(uid) && rlGame.players.size < rlGame.maxPlayers) {
        rlGame.players.set(uid, { name: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null, alive: true });
        broadcast(key, 'roulette:joined', { player: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null, count: rlGame.players.size, max: rlGame.maxPlayers });
      }
    }
    if (cwGame.active && data.comment) {
      const comment = data.comment.trim().toLowerCase().replace(/\s+/g,'');
      const uid = data.userId || data.uniqueId;
      // Registration: keyword + أحمر/أزرق or 1/2 (مع توحيد الهمزات)
      if (cwGame.phase === 'register' && !cwGame.playerTeam.has(uid)) {
        const normC = comment.replace(/[أإآ]/g, 'ا');
        let team = null;
        if (normC.includes('احمر') || comment === '1') team = 'red';
        else if (normC.includes('ازرق') || comment === '2') team = 'blue';
        if (team) {
          cwGame.playerTeam.set(uid, team);
          cwGame[team].players.add(uid);
          broadcast(key, 'castle:joined', {
            player: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null,
            team, redCount: cwGame.red.players.size, blueCount: cwGame.blue.players.size,
          });
        }
      }
      // Battle: any comment = attack enemy castle
      if (cwGame.phase === 'battle' && cwGame.playerTeam.has(uid)) {
        const team = cwGame.playerTeam.get(uid);
        const enemy = team === 'red' ? 'blue' : 'red';
        cwGame[team].attacks++;
        cwGame[enemy].hp = Math.max(0, cwGame[enemy].hp - 1);
        broadcast(key, 'castle:attack', {
          attacker: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null,
          team, redHP: cwGame.red.hp, blueHP: cwGame.blue.hp,
          redAttacks: cwGame.red.attacks, blueAttacks: cwGame.blue.attacks,
          maxHP: cwGame.maxHP,
        });
        // Check if castle destroyed
        if (cwGame[enemy].hp <= 0 && cwGame.phase === 'battle') {
          cwGame.phase = 'finished';
          cwGame.active = false;
          broadcast(key, 'castle:finish', {
            winner: team, redHP: cwGame.red.hp, blueHP: cwGame.blue.hp,
            redAttacks: cwGame.red.attacks, blueAttacks: cwGame.blue.attacks,
          });
          console.log(`[Castle] ${team} wins! Attacks: R${cwGame.red.attacks} B${cwGame.blue.attacks}`);
        }
      }
    }
    if (hrGame.active && data.comment) {
      const comment = data.comment.trim();
      const uid = data.userId || data.uniqueId;
      // Registration: type 1,2,3,4 to join team
      if (hrGame.phase === 'register' && !hrGame.playerTeam.has(uid)) {
        const teamNum = parseInt(comment);
        if (teamNum >= 1 && teamNum <= 4) {
          hrGame.playerTeam.set(uid, teamNum);
          hrGame.teams[teamNum].players.add(uid);
          const teamCounts = {};
          for (let i = 1; i <= 4; i++) teamCounts[i] = hrGame.teams[i].players.size;
          broadcast(key, 'horse:joined', {
            player: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null,
            team: teamNum, teamCounts,
          });
        }
      }
      // Racing: any comment from a registered player moves their horse
      if (hrGame.phase === 'racing' && hrGame.playerTeam.has(uid)) {
        const teamNum = hrGame.playerTeam.get(uid);
        const team = hrGame.teams[teamNum];
        team.progress++;
        const progress = {};
        for (let i = 1; i <= 4; i++) progress[i] = hrGame.teams[i].progress;
        broadcast(key, 'horse:move', { team: teamNum, progress, finishLine: hrGame.finishLine });
        // Check winner
        if (team.progress >= hrGame.finishLine && hrGame.phase === 'racing') {
          hrGame.phase = 'finished';
          hrGame.active = false;
          broadcast(key, 'horse:finish', { winner: teamNum, teamName: team.name, teamIcon: team.icon, progress });
          console.log(`[Horse] Team ${team.name} wins!`);
        }
      }
    }
    if (iceGame.active && data.comment) {
      const comment = data.comment.trim().toLowerCase().replace(/\s+/g,'');
      const uid = data.userId || data.uniqueId;
      // Registration
      if (iceGame.phase === 'register' && comment === iceGame.keyword && !iceGame.players.has(uid) && iceGame.players.size < (iceGame.maxPlayers || 100)) {
        iceGame.players.set(uid, { name: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null, frozen: false, unfreezeTime: 0 });
        broadcast(key, 'ice:joined', { player: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null, count: iceGame.players.size, max: iceGame.maxPlayers || 20 });
        console.log(`[Ice] ${data.nickname} joined (${iceGame.players.size})`);
      }
      // Unfreeze
      if (iceGame.phase === 'frozen' && iceGame.players.has(uid)) {
        const player = iceGame.players.get(uid);
        if (player.frozen && comment === iceGame.word) {
          player.frozen = false;
          player.unfreezeTime = Date.now();
          const stillFrozen = Array.from(iceGame.players.values()).filter(p => p.frozen && !iceGame.losers.find(l => l.name === p.name)).length;
          broadcast(key, 'ice:unfreeze', { player: player.name, avatar: player.avatar, stillFrozen });
          console.log(`[Ice] ${player.name} unfroze! ${stillFrozen} still frozen`);
        }
      }
    }
    if (lrGame.active && lrGame.phase === 'racing' && data.comment) {
      const word = data.comment.trim();
      const uid = data.userId || data.uniqueId;
      // Check if word starts with the correct letter and is at least 2 chars
      if (word.length >= 2 && word.charAt(0) === lrGame.letter) {
        const wordKey = word.toLowerCase().replace(/\s+/g,'');
        if (!lrGame.usedWords.has(wordKey)) {
          lrGame.usedWords.add(wordKey);
          // Get or create player
          if (!lrGame.players.has(uid)) {
            lrGame.players.set(uid, { name: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null, words: [], finished: false, finishTime: 0 });
          }
          const player = lrGame.players.get(uid);
          if (!player.finished) {
            player.words.push(word);
            broadcast(key, 'letter-race:word', {
              player: player.name, avatar: player.avatar, word,
              playerWords: player.words.length, needed: lrGame.wordsNeeded,
            });
            // Check if player finished
            if (player.words.length >= lrGame.wordsNeeded) {
              player.finished = true;
              player.finishTime = Date.now();
              const rank = lrGame.winners.length + 1;
              lrGame.winners.push({ name: player.name, avatar: player.avatar, rank, words: [...player.words] });
              broadcast(key, 'letter-race:winner', {
                player: player.name, avatar: player.avatar, rank,
                words: player.words, total: lrGame.winners.length,
              });
              console.log(`[LetterRace] ${player.name} finished #${rank}! Words: ${player.words.join(', ')}`);
            }
          }
        }
      }
    }
    if (drawGame.active && drawGame.phase === 'drawing' && data.comment) {
      const guess = data.comment.trim().toLowerCase().replace(/\s+/g,'');
      const uid = data.userId || data.uniqueId;
      if (guess === drawGame.word && !drawGame.guessedUsers.has(uid)) {
        drawGame.guessedUsers.add(uid);
        const winner = { name: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null, rank: drawGame.winners.length + 1 };
        drawGame.winners.push(winner);
        broadcast(key, 'draw:correct', { player: winner.name, avatar: winner.avatar, rank: winner.rank, total: drawGame.winners.length });
        console.log(`[Draw] ${winner.name} guessed correctly! (#${winner.rank})`);
      }
    }
    if (koGame.active && data.comment) {
      const comment = data.comment.trim().toLowerCase().replace(/\s+/g,'');
      const uid = data.userId || data.uniqueId;
      // Registration
      if (koGame.phase === 'register' && !koGame.registrationLocked && comment === koGame.keyword && !koGame.players.has(uid) && koGame.players.size < koGame.maxPlayers) {
        koGame.players.set(uid, { name: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null, alive: true });
        broadcast(key, 'knockout:joined', { player: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null, count: koGame.players.size, max: koGame.maxPlayers });
        console.log(`[Knockout] ${data.nickname} joined (${koGame.players.size}/${koGame.maxPlayers})`);
      }
      // Answering
      if (koGame.phase === 'question') {
        const answer = parseInt(data.comment.trim());
        if (answer >= 1 && answer <= 4 && koGame.players.has(uid) && koGame.players.get(uid).alive && !koGame.answers.has(uid)) {
          koGame.answers.set(uid, { name: data.nickname || data.uniqueId, answer });
          broadcast(key, 'knockout:answered', { count: koGame.answers.size, total: Array.from(koGame.players.values()).filter(p=>p.alive).length });
        }
      }
    }
    if (milGame.active && milGame.phase === 'question' && data.comment) {
      const answer = parseInt(data.comment.trim());
      const uid = data.userId || data.uniqueId;
      if (answer >= 1 && answer <= 4 && !milGame.answers.has(uid)) {
        milGame.answers.set(uid, {
          name: data.nickname || data.uniqueId,
          avatar: data.profilePictureUrl || null,
          answer,
        });
        broadcast(key, 'million:answered', { count: milGame.answers.size });
      }
    }
    if (memGame.active && data.comment) {
      const comment = data.comment.trim().toLowerCase().replace(/\s+/g,'');
      const uid = data.userId || data.uniqueId;

      // Registration phase
      if (memGame.phase === 'register' && comment === memGame.keyword && !memGame.players.has(uid)) {
        memGame.players.set(uid, {
          name: data.nickname || data.uniqueId,
          avatar: data.profilePictureUrl || null,
          alive: true, score: 0,
        });
        broadcast(key, 'memory:player-joined', {
          player: data.nickname || data.uniqueId,
          avatar: data.profilePictureUrl || null,
          totalPlayers: memGame.players.size,
        });
        console.log(`[Memory] ${data.nickname} joined (${memGame.players.size} total)`);
      }

      // Answering phase
      if (memGame.phase === 'answering' && memGame.players.has(uid)) {
        const player = memGame.players.get(uid);
        if (player.alive && !memGame.roundAnswers.has(uid)) {
          memGame.roundAnswers.add(uid);
          // Normalize: remove all whitespace, normalize Arabic letters
          const normalize = (s) => String(s||'').replace(/\s+/g,'').replace(/[إأآا]/g,'ا').replace(/ى/g,'ي').replace(/ة/g,'ه').toLowerCase();
          const answer = normalize(data.comment);
          const expected = normalize(memGame.sequence);
          const correct = answer === expected;
          console.log(`[Memory] ${player.name}: "${data.comment}" → "${answer}" vs "${expected}" = ${correct}`);
          if (correct) {
            player.score++;
            broadcast(key, 'memory:correct', { player: player.name, avatar: player.avatar, score: player.score });
          } else {
            player.alive = false;
            memGame.eliminated.push({ name: player.name, avatar: player.avatar, round: memGame.round, reason: 'إجابة خاطئة' });
            broadcast(key, 'memory:wrong', { player: player.name, avatar: player.avatar, answer: data.comment, correct: memGame.sequence });
          }
        }
      }
    }

    // Check Word War game (حرب الكلمات)
    const wwGame = getWordWarGame(key);
    if (data.comment && (wwGame.active || wwGame.registrationOpen)) {
      const word = data.comment.trim().toLowerCase().replace(/\s+/g,'');
      const uid = data.userId || data.uniqueId;

      // Check if this is a team registration keyword
      if (!wwGame.registrationLocked && (word === wwGame.redKeyword || word === wwGame.blueKeyword) && !wwGame.redTeam.has(uid) && !wwGame.blueTeam.has(uid)) {
        const team = word === wwGame.redKeyword ? 'red' : 'blue';
        const teamMap = team === 'red' ? wwGame.redTeam : wwGame.blueTeam;
        teamMap.set(uid, { name: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null, words: [], totalWords: 0 });
        broadcast(key, 'word-war:join', { team, player: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null, redCount: wwGame.redTeam.size, blueCount: wwGame.blueTeam.size });
        console.log(`[WordWar] ${data.nickname} joined ${team.toUpperCase()}`);
      }
      // Check if player is in a team and game is active
      else if (wwGame.active) {
        let team = null;
        if (wwGame.redTeam.has(uid)) team = 'red';
        else if (wwGame.blueTeam.has(uid)) team = 'blue';

        if (team) {
          const isValid = wwGame.validWordsSet.size === 0 || wwGame.validWordsSet.has(word);
          if (isValid && word.length >= 2) {
            const teamWords = team === 'red' ? wwGame.redWords : wwGame.blueWords;
            const oppositeWords = team === 'red' ? wwGame.blueWords : wwGame.redWords;
            const teamMap = team === 'red' ? wwGame.redTeam : wwGame.blueTeam;

            if (!teamWords.has(word) && !oppositeWords.has(word)) {
              teamWords.add(word);
              const player = teamMap.get(uid);
              if (player) {
                player.words.push(word);
                player.totalWords = (player.totalWords || 0) + 1;
              }
              if (team === 'red') wwGame.redScore++; else wwGame.blueScore++;

              broadcast(key, 'word-war:word', {
                team, word: data.comment.trim(),
                player: data.nickname || data.uniqueId,
                avatar: data.profilePictureUrl || null,
                redScore: wwGame.redScore, blueScore: wwGame.blueScore,
                totalWords: player ? player.totalWords : 0,
              });
            }
          }
        }
      }
    }
    if (swGame.active && !swGame.winner && !swGame.revealed && data.comment) {
      const userAnswer = normalizePersonAnswer(data.comment);
      const correctAnswer = normalizePersonAnswer(swGame.word);
      const allAnswers = [correctAnswer, ...(swGame.aliases || []).map(a => normalizePersonAnswer(a))];
      if (allAnswers.includes(userAnswer)) {
        const uid = data.userId || data.uniqueId;
        const stats = swGame.winners.get(uid) || {
          name: data.nickname || data.uniqueId,
          avatar: data.profilePictureUrl || null,
          uniqueId: data.uniqueId,
          totalWins: 0,
        };
        stats.totalWins += 1;
        stats.name = data.nickname || data.uniqueId;
        stats.avatar = data.profilePictureUrl || null;
        swGame.winners.set(uid, stats);

        swGame.winner = {
          userId: uid, name: stats.name, avatar: stats.avatar,
          uniqueId: data.uniqueId, totalWins: stats.totalWins,
        };
        swGame.revealed = true;
        swGame.active = false;
        if (swGame.revealTimer) clearInterval(swGame.revealTimer);
        if (swGame.endTimer) clearTimeout(swGame.endTimer);

        console.log(`[SecretWord] Winner: ${stats.name} - "${data.comment}"`);
        broadcast(key, 'secret-word:winner', swGame.winner);
        broadcast(key, 'secret-word:reveal', { word: swGame.word });
      }
    }
    if (pcGame.active && !pcGame.winner && !pcGame.revealed && data.comment) {
      const userAnswer = normalizePersonAnswer(data.comment);
      const correctAnswer = normalizePersonAnswer(pcGame.answer);
      // اقبل الإجابة الأساسية أو أي من الـ aliases
      const allAnswers = [correctAnswer, ...(pcGame.aliases || []).map(a => normalizePersonAnswer(a))];
      if (allAnswers.includes(userAnswer)) {
        const uid = data.userId || data.uniqueId;
        const stats = pcGame.winners.get(uid) || {
          name: data.nickname || data.uniqueId,
          avatar: data.profilePictureUrl || null,
          uniqueId: data.uniqueId,
          totalWins: 0,
        };
        stats.totalWins += 1;
        stats.name = data.nickname || data.uniqueId;
        stats.avatar = data.profilePictureUrl || null;
        pcGame.winners.set(uid, stats);

        pcGame.winner = {
          userId: uid,
          name: stats.name,
          avatar: stats.avatar,
          uniqueId: data.uniqueId,
          totalWins: stats.totalWins,
        };
        pcGame.revealed = true;
        pcGame.active = false;
        if (pcGame.timer) { clearTimeout(pcGame.timer); pcGame.timer = null; }

        console.log(`[PhotoChallenge] Winner: ${stats.name} - "${data.comment}"`);
        broadcast(key, 'photo-challenge:winner', pcGame.winner);
        broadcast(key, 'photo-challenge:reveal', { answer: pcGame.answer });
      }
    }

    // Check person game (خمّن الشخصية)
    const personGame = getPersonGame(key);
    if (personGame.active && !personGame.transitioning &&
        personGame.winners.length < 5 &&
        personGame.person && data.comment) {
      const answerClean = normalizePersonAnswer(data.comment);
      const names = [personGame.person.name, ...(personGame.person.aliases || [])];
      const match = names.some(n => normalizePersonAnswer(n) === answerClean);
      const alreadyWon = personGame.winners.some(w => w.userId === data.userId);
      console.log(`[Person] "${data.comment}" vs ${personGame.person.name} = ${match}`);
      if (match && !alreadyWon) {
        const uid = data.userId || data.uniqueId;
        const existing = personGame.playerStats.get(uid) || { name: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null, totalPersons: 0 };
        existing.totalPersons += 1;
        existing.name = data.nickname || data.uniqueId;
        existing.avatar = data.profilePictureUrl || null;
        personGame.playerStats.set(uid, existing);
        const winner = {
          userId: uid,
          name: existing.name,
          avatar: existing.avatar,
          rank: personGame.winners.length + 1,
          person: personGame.person.name,
          totalPersons: existing.totalPersons,
        };
        personGame.winners.push(winner);
        console.log(`[Person] Winner #${winner.rank}: ${winner.name}`);
        const allPlayers = Array.from(personGame.playerStats.entries())
          .map(([uid, s]) => ({ userId: uid, name: s.name, avatar: s.avatar, totalPersons: s.totalPersons }))
          .sort((a,b) => b.totalPersons - a.totalPersons);
        broadcast(key, 'person:won', { winner, winners: personGame.winners, allPlayers });
        if (personGame.winners.length === 5 && !personGame.transitioning) {
          personGame.active = false;
          personGame.transitioning = true;
          broadcast(key, 'person:reveal', { answer: personGame.person.name, image: personGame.person.image });
        }
      }
    }

    // Check Mystery Box game
    const mysteryGame = getMysteryGame(key);
    if (mysteryGame.active && mysteryGame.currentBoxId && data.comment) {
      const box = mysteryGame.boxes.find(b => b.id === mysteryGame.currentBoxId);
      if (box && box.winners.length < mysteryGame.maxWinners) {
        const answerClean = normalizePersonAnswer(data.comment);
        const isCorrect = box.answersClean.includes(answerClean);
        const alreadyWon = box.winners.some(w => w.userId === data.userId);

        // حفظ الإجابة في recentAnswers
        mysteryGame.recentAnswers.push({
          userId: data.userId,
          name: data.nickname || data.uniqueId,
          avatar: data.profilePictureUrl || null,
          answer: data.comment,
          correct: isCorrect,
          ts: Date.now(),
        });
        if (mysteryGame.recentAnswers.length > 10) mysteryGame.recentAnswers.shift();

        broadcast(key, 'mystery:answer', {
          recentAnswers: mysteryGame.recentAnswers,
        });

        if (isCorrect && !alreadyWon) {
          const uid = data.userId || data.uniqueId;
          const stats = mysteryGame.playerStats.get(uid) || { name: data.nickname || data.uniqueId, avatar: data.profilePictureUrl || null, totalWins: 0 };
          stats.totalWins += 1;
          stats.name = data.nickname || data.uniqueId;
          stats.avatar = data.profilePictureUrl || null;
          mysteryGame.playerStats.set(uid, stats);

          const winner = {
            userId: uid,
            name: stats.name,
            avatar: stats.avatar,
            answer: data.comment,
            rank: box.winners.length + 1,
            totalWins: stats.totalWins,
          };
          box.winners.push(winner);
          console.log(`[Mystery] Box ${box.id} winner #${winner.rank}: ${winner.name} - "${data.comment}"`);

          broadcast(key, 'mystery:won', {
            boxId: box.id,
            winner,
            winners: box.winners,
          });

          // لو وصل maxWinners، انتهى الصندوق
          if (box.winners.length >= mysteryGame.maxWinners) {
            mysteryGame.active = false;
            if (mysteryGame.boxTimer) { clearTimeout(mysteryGame.boxTimer); mysteryGame.boxTimer = null; }
            broadcast(key, 'mystery:timeUp', {
              boxId: box.id,
              challenge: box.challenge,
              answers: box.answers,
              winners: box.winners,
            });
          }
        }
      }
    }

    // Check countdown game (العدّ التنازلي)
    const cdGame = getCountdownGame(key);
    if (cdGame.active && data.comment) {
      const answerClean = normalizePersonAnswer(data.comment);
      const isCorrect = cdGame.answers.includes(answerClean);
      const alreadyWon = cdGame.winners.some(w => w.userId === data.userId);

      // حفظ الإجابة في recentAnswers (آخر 10)
      cdGame.recentAnswers.push({
        userId: data.userId,
        name: data.nickname || data.uniqueId,
        avatar: data.profilePictureUrl || null,
        answer: data.comment,
        correct: isCorrect,
        ts: Date.now(),
      });
      if (cdGame.recentAnswers.length > 10) cdGame.recentAnswers.shift();

      broadcast(key, 'countdown:answer', {
        recentAnswers: cdGame.recentAnswers,
      });

      if (isCorrect && !alreadyWon) {
        const winner = {
          userId: data.userId,
          name: data.nickname || data.uniqueId,
          avatar: data.profilePictureUrl || null,
          answer: data.comment,
          rank: cdGame.winners.length + 1,
          time: Date.now() - cdGame.startTime,
        };
        cdGame.winners.push(winner);
        console.log(`[Countdown] Winner #${winner.rank}: ${winner.name} - "${data.comment}"`);
        broadcast(key, 'countdown:won', {
          winner,
          totalWinners: cdGame.winners.length,
        });
      }
    }

    // Check word game
    const game = getGame(key);
    if (game.active && game.word && data.comment &&
        data.comment.trim().toLowerCase() === game.word) {
      const winner = {
        userId: data.userId,
        name: data.nickname || data.uniqueId,
        avatar: data.profilePictureUrl || null,
        time: Date.now(),
        count: (game.winners.get(data.userId)?.count || 0) + 1,
      };
      game.winners.set(data.userId, winner);
      broadcast(key, 'word:update', {
        winners: Array.from(game.winners.values()),
        count: game.winners.size,
        newWinner: winner,
      });
    }
  });

  tiktok.on('like', (data) => {
    const usr = u(data);
    if (data.totalLikeCount || data.totalLikes) room.stats.likes = data.totalLikeCount || data.totalLikes;
    broadcast(key, 'like', { user: usr.nickname, totalLikeCount: data.totalLikeCount || data.totalLikes });
    broadcast(key, 'stats', room.stats);

    // Check Auction (مزاد الكلمات)
    const auctionGame = getAuction(key);
    if (auctionGame.active && auctionGame.phase === 'bidding') {
      const uid = usr.userId || usr.uniqueId;
      const likes = data.likeCount || 1;
      if (!auctionGame.bids.has(uid)) {
        auctionGame.bids.set(uid, { name: usr.nickname, avatar: usr.profilePictureUrl, likes: 0 });
      }
      const bidder = auctionGame.bids.get(uid);
      bidder.likes += likes;
      const sorted = Array.from(auctionGame.bids.values()).sort((a,b) => b.likes - a.likes);
      const top5 = sorted.slice(0, 5);
      broadcast(key, 'auction:bid', { player: bidder.name, avatar: bidder.avatar, likes: bidder.likes, top5, totalBids: auctionGame.bids.size });
    }
  });

  tiktok.on('gift', (data) => {
    const usr = u(data);
    if (data.giftType === 1 && !data.repeatEnd) return;
    room.stats.diamonds = (room.stats.diamonds || 0) + 1; // Count gifts
    const msg = {
      type: 'gift', user: usr.nickname, avatar: usr.profilePictureUrl,
      giftName: data.giftName || data.gift?.name || 'Gift',
      giftId: data.giftId || data.gift?.giftId,
      giftImageUrl: data.giftPictureUrl || data.gift?.image?.url || null,
      repeatCount: data.repeatCount || 1,
      diamondCount: data.diamondCount || data.gift?.diamondCount || 0,
      ts: Date.now(),
    };
    storeMsg(key, msg);
    broadcast(key, 'gift', msg);
    broadcast(key, 'stats', room.stats);

    // خمن الوقت: مرسل الهدية أثناء الجولة يصبح "مؤهل"
    const gtG = getGuessTime(key);
    if (['visible', 'hidden', 'guessing'].includes(gtG.state)) {
      const guid = data.userId || data.uniqueId || usr.nickname;
      if (guid && !gtG.eligible.has(guid)) {
        gtG.eligible.add(guid);
        broadcast(key, 'gtime:eligible', { eligibleCount: gtG.eligible.size });
      }
    }
  });

  tiktok.on('member', (data) => {
    const usr = u(data);
    const msg = { type: 'member', user: usr.nickname, avatar: usr.profilePictureUrl, actionId: data.actionId, ts: Date.now() };
    if (data.actionId === 1) storeMsg(key, msg);
    broadcast(key, 'member', msg);
  });

  // Social event splits into follow/share
  tiktok.on('social', (data) => {
    const usr = u(data);
    const action = data.common?.displayText?.defaultPattern || data.displayType || '';
    const isFollow = action.includes('follow') || action.includes('متابع') || data.action === 1;
    const isShare = action.includes('share') || action.includes('مشاركة') || data.action === 3;
    
    if (isFollow || (!isShare)) {
      // Treat as follow by default
      tiktok.emit('follow', data);
    }
    if (isShare) {
      tiktok.emit('share', data);
    }
  });

  tiktok.on('follow', (data) => {
    const usr = u(data);
    const uid = usr.userId || usr.uniqueId;
    const isNew = uid && !room.followerSet.has(uid);
    if (isNew) {
      room.followerSet.add(uid);
      room.stats.followers = room.followerSet.size;
      broadcast(key, 'stats', room.stats);
    }
    const msg = { type: 'follow', user: usr.nickname, avatar: usr.profilePictureUrl, ts: Date.now(), isNew };
    storeMsg(key, msg);
    broadcast(key, 'follow', msg);
  });

  tiktok.on('share', (data) => {
    const usr = u(data);
    room.stats.shares = (room.stats.shares || 0) + 1;
    const msg = { type: 'share', user: usr.nickname, avatar: usr.profilePictureUrl, ts: Date.now() };
    storeMsg(key, msg);
    broadcast(key, 'share', msg);
    broadcast(key, 'stats', room.stats);
  });

  tiktok.on('emote', (data) => {
    const emote = data.emoteList?.[0];
    if (!emote) return;
    broadcast(key, 'emote', {
      user: data.user?.nickname || data.user?.uniqueId || '؟',
      emoteId: emote.emoteId,
      emoteImageUrl: emote.image?.imageUrl || null,
      ts: Date.now(),
    });
  });

  // Viewers - handle both event names and EulerStream format
  const handleViewers = (data) => {
    const count = data.viewerCount || data.total || data.totalUser || 0;
    if (count) room.stats.viewers = count;
    broadcast(key, 'viewers', { count });
    broadcast(key, 'stats', room.stats);
  };
  tiktok.on('roomUserSeq', handleViewers);
  tiktok.on('roomUser', handleViewers);

  // Room info for initial stats
  tiktok.on('roomInfo', (data) => {
    const info = data.roomInfo || data;
    // Debug first roomInfo
    if (!room._roomInfoDebug) {
      room._roomInfoDebug = true;
      console.log(`[RoomInfo] @${key}: ${JSON.stringify(info).slice(0, 300)}`);
    }
    const viewers = info.userCount || info.user_count || info.viewerCount || data.viewerCount || 0;
    if (viewers > 0) room.stats.viewers = viewers;
    const likes = info.likeCount || info.like_count || data.totalLikeCount || 0;
    if (likes > 0) room.stats.likes = likes;
    broadcast(key, 'stats', room.stats);
  });

  tiktok.on('streamEnd', () => {
    console.log(`[TikTok] Stream ended @${key}`);
    room.status = 'ended';
    io.emit('room:status', { username: key, status: 'ended' });
    try { ws.close(); } catch(e) {}
  });
}

// ⛔ scheduleRetry حُذفت نهائياً — الاعتماد على scheduleReconnect المنضبطة فقط

function storeMsg(key, msg) {
  const room = rooms[key];
  if (!room) return;
  room.messages.push(msg);
  if (room.messages.length > MAX_STORED) room.messages.shift();
}

// ── Wheel Store ───────────────────────────────────────────
// wheels[username] = { keyword, entries: Set of {userId, name, avatar} }
const wheels = {};

function getWheel(key) {
  if (!wheels[key]) wheels[key] = {
    keyword: 'اشتراك',
    entries: new Map(),
    accepting: false,      // ما يقبل أسماء جديدة من chat إلا لما تضغط ابدأ
    removedIds: new Set()  // للحذف النهائي - حتى لو كتب الشخص مرة ثانية ما يدخل
  };
  return wheels[key];
}

// REST API for wheel
app.post('/api/wheel/config', (req, res) => {
  const { username, keyword } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  getWheel(key).keyword = keyword || 'اشتراك';
  res.json({ ok: true, keyword: getWheel(key).keyword });
});

app.post('/api/wheel/clear', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const wheel = getWheel(key);
  wheel.entries.clear();
  wheel.removedIds.clear();
  io.to(`room:${key}`).emit('wheel:update', { entries: [], count: 0, fullSync: true });
  res.json({ ok: true });
});

app.post('/api/wheel/add', (req, res) => {
  const { username, name } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !name) return res.json({ ok: false });
  const wheel = getWheel(key);
  const userId = 'manual_' + Date.now();
  const entry = { userId, name: name.trim(), avatar: null };
  wheel.entries.set(userId, entry);
  io.to(`room:${key}`).emit('wheel:update', {
    entries: Array.from(wheel.entries.values()),
    count: wheel.entries.size,
    newEntry: entry,
  });
  res.json({ ok: true, entry });
});

app.post('/api/wheel/start-registration', (req, res) => {
  const { username, duration } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const wheel = getWheel(key);
  wheel.accepting = true;
  const dur = parseInt(duration) || 0;
  const endTime = dur > 0 ? Date.now() + dur * 1000 : 0;
  wheel.regEndTime = endTime;
  // إلغاء timer سابق
  if (wheel.regTimer) clearTimeout(wheel.regTimer);
  if (dur > 0) {
    wheel.regTimer = setTimeout(() => {
      wheel.accepting = false;
      wheel.regEndTime = 0;
      io.to(`room:${key}`).emit('wheel:registration', { accepting: false, endTime: 0 });
    }, dur * 1000);
  }
  io.to(`room:${key}`).emit('wheel:registration', {
    accepting: true,
    endTime,
    keyword: wheel.keyword || 'اشتراك',
    count: wheel.entries.size,
  });
  res.json({ ok: true });
});

app.post('/api/wheel/stop-registration', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const wheel = getWheel(key);
  wheel.accepting = false;
  wheel.regEndTime = 0;
  if (wheel.regTimer) { clearTimeout(wheel.regTimer); wheel.regTimer = null; }
  io.to(`room:${key}`).emit('wheel:registration', { accepting: false, endTime: 0 });
  res.json({ ok: true });
});

app.post('/api/wheel/remove-winner', (req, res) => {
  const { username, userId } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !userId) return res.json({ ok: false });
  const wheel = getWheel(key);
  wheel.entries.delete(userId);
  io.to(`room:${key}`).emit('wheel:update', {
    entries: Array.from(wheel.entries.values()),
    count: wheel.entries.size,
    fullSync: true,
  });
  res.json({ ok: true });
});

app.post('/api/wheel/remove', (req, res) => {
  const { username, userId } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !userId) return res.json({ ok: false });
  const wheel = getWheel(key);
  wheel.entries.delete(userId);
  io.to(`room:${key}`).emit('wheel:update', {
    entries: Array.from(wheel.entries.values()),
    count: wheel.entries.size,
    fullSync: true,
  });
  res.json({ ok: true });
});

app.post('/api/wheel/spin', (req, res) => {
  const { username, duration, speed } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const wheel = getWheel(key);
  if (wheel.entries.size < 2) return res.json({ ok: false, message: 'يحتاج مشتركين أكثر' });
  const entries = Array.from(wheel.entries.values());
  const winnerIndex = Math.floor(Math.random() * entries.length);
  const winner = entries[winnerIndex];
  const durationMs = (duration || 5) * 1000;
  io.to(`room:${key}`).emit('wheel:spin', {
    winner, winnerIndex, duration: durationMs,
    speed: speed || 'normal', entries
  });
  res.json({ ok: true, winner });
});

app.get('/api/wheel/:username', (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const wheel = getWheel(key);
  res.json({
    keyword: wheel.keyword,
    entries: Array.from(wheel.entries.values()),
    count: wheel.entries.size,
    accepting: wheel.accepting,
    regEndTime: wheel.regEndTime || 0,
  });
});

// ── Guess Game Store ──────────────────────────────────────
const guessGames = {};

function getGuessGame(key) {
  if (!guessGames[key]) guessGames[key] = {
    word: '', hint: '', active: false, winner: null,
    revealed: [], winners: [],
    playerStats: new Map(),
    autoMode: false,
    wordPool: [],
    usedWords: new Set(), // الكلمات المستخدمة في الجلسة الحالية
    scale: 1,
    transitionDelay: 5000,
    pendingTransition: null,
  };
  return guessGames[key];
}

function schedulenextWord(key, delay) {
  const game = getGuessGame(key);
  if (game.pendingTransition) clearTimeout(game.pendingTransition);
  game.pendingTransition = setTimeout(() => goToNextWord(key), delay);
}

function goToNextWord(key) {
  const game = getGuessGame(key);
  game.pendingTransition = null;
  game.transitioning = false;
  // فلترة الكلمات: تجنب الكلمة الحالية + الكلمات المستخدمة في الجلسة
  let pool = (game.wordPool && game.wordPool.length)
    ? game.wordPool.filter(w => w.w !== game.word && !game.usedWords.has(w.w))
    : [];
  // لو خلصت كل الكلمات، صفّر الـ used وابدأ جولة جديدة
  if (!pool.length && game.wordPool && game.wordPool.length) {
    console.log(`[Guess] All words used (${game.usedWords.size}), resetting cycle`);
    game.usedWords.clear();
    pool = game.wordPool.filter(w => w.w !== game.word);
  }
  if (!pool.length) {
    game.winners = [];
    game.active = true;
    io.to(`room:${key}`).emit('guess:started', {
      length: game.word.length, hint: game.hint,
      revealed: game.revealed,
      letters: game.revealed.map(i => ({ i, c: game.word[i] })),
      winners: [],
    });
    return;
  }
  const next = pool[Math.floor(Math.random() * pool.length)];
  game.word    = next.w;
  game.hint    = next.h || '';
  game.active  = true;
  game.winners = [];
  game.usedWords.add(next.w); // أضفها للمستخدمة
  const indices = [...Array(next.w.length).keys()];
  const revealCount = Math.max(1, Math.floor(next.w.length * 0.3));
  game.revealed = indices.sort(() => Math.random() - 0.5).slice(0, revealCount).sort((a,b) => a-b);
  io.to(`room:${key}`).emit('guess:started', {
    length: game.word.length, hint: game.hint,
    revealed: game.revealed,
    letters: game.revealed.map(i => ({ i, c: game.word[i] })),
    winners: [],
  });
  console.log(`[Guess] Next word: ${next.w} (${game.usedWords.size}/${game.wordPool.length})`);
}

// زر "التالي" - انتقال فوري
app.post('/api/guess/next', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getGuessGame(key);
  if (game.pendingTransition) { clearTimeout(game.pendingTransition); game.pendingTransition = null; }
  goToNextWord(key);
  res.json({ ok: true });
});

// تحديث مدة الانتقال
app.post('/api/guess/delay', (req, res) => {
  const { username, delay } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getGuessGame(key);
  game.transitionDelay = Math.max(0, Math.min(60000, parseInt(delay) || 5000));
  res.json({ ok: true, delay: game.transitionDelay });
});

app.post('/api/guess/start', (req, res) => {
  const { username, word, hint, wordPool } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !word) return res.json({ ok: false });
  const game = getGuessGame(key);
  // ألغي أي انتقال معلق من الجولة السابقة
  if (game.pendingTransition) { clearTimeout(game.pendingTransition); game.pendingTransition = null; }
  game.transitioning = false;
  // لو بدء جديد (لم تكن نشطة)، صفّر الكلمات المستخدمة
  if (!game.active) {
    game.usedWords = new Set();
  }
  game.word    = word.trim();
  game.hint    = hint || '';
  game.active  = true;
  game.winner  = null;
  game.winners = []; // reset round winners only, keep playerStats
  game.usedWords.add(game.word); // اعتبرها مستخدمة
  if (wordPool && Array.isArray(wordPool)) game.wordPool = wordPool;
  // Reveal ~30% of letters randomly
  const indices = [...Array(word.length).keys()];
  const revealCount = Math.max(1, Math.floor(word.length * 0.3));
  game.revealed = indices.sort(() => Math.random() - 0.5).slice(0, revealCount).sort((a,b) => a-b);
  io.to(`room:${key}`).emit('guess:started', {
    length: game.word.length, hint: game.hint, revealed: game.revealed,
    letters: game.revealed.map(i => ({ i, c: game.word[i] })),
    winners: [],
  });
  console.log(`[Guess] Started: ${game.word} (used: ${game.usedWords.size}/${game.wordPool.length})`);
  res.json({ ok: true });
});

app.post('/api/guess/auto', (req, res) => {
  const { username, enabled, wordPool } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getGuessGame(key);
  game.autoMode = !!enabled;
  if (wordPool && Array.isArray(wordPool)) game.wordPool = wordPool;
  res.json({ ok: true });
});

app.post('/api/guess/scale', (req, res) => {
  const { username, scale } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getGuessGame(key);
  game.scale = Math.max(0.6, Math.min(2, parseFloat(scale) || 1));
  io.to(`room:${key}`).emit('guess:scale', { scale: game.scale });
  res.json({ ok: true, scale: game.scale });
});

app.post('/api/guess/clear-stats', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  getGuessGame(key).playerStats.clear();
  res.json({ ok: true });
});

app.post('/api/guess/clear-winners', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  getGuessGame(key).winners = [];
  io.to(`room:${key}`).emit('guess:winners', { winners: [] });
  res.json({ ok: true });
});

app.post('/api/guess/stop', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getGuessGame(key);
  game.active = false;
  game.autoMode = false; // أيضاً أوقف الـ auto mode
  game.usedWords = new Set(); // reset for next session
  const top5 = Array.from(game.playerStats.entries())
    .map(([uid, s]) => ({ userId: uid, name: s.name, avatar: s.avatar, totalWords: s.totalWords }))
    .sort((a,b) => b.totalWords - a.totalWords)
    .slice(0, 5);
  io.to(`room:${key}`).emit('guess:stopped', { word: game.word, top5 });
  res.json({ ok: true });
});

app.get('/api/guess/:username', (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const game = getGuessGame(key);
  const allPlayers = Array.from(game.playerStats.entries())
    .map(([userId, s]) => ({ userId, name: s.name, avatar: s.avatar, totalWords: s.totalWords }))
    .sort((a,b) => b.totalWords - a.totalWords);
  res.json({
    word: game.word, hint: game.hint, active: game.active,
    winner: game.winner, winners: game.winners || [],
    revealed: game.revealed,
    letters: game.revealed.map(i => ({ i, c: game.word[i] })),
    length: game.word.length,
    allPlayers,
    scale: game.scale || 1,
    transitionDelay: game.transitionDelay || 5000,
  });
});

// ── Word Game Store ───────────────────────────────────────
const wordGames = {};

function getGame(key) {
  if (!wordGames[key]) wordGames[key] = { word: '', active: false, winners: new Map() };
  return wordGames[key];
}

app.post('/api/word/start', (req, res) => {
  const { username, word } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !word) return res.json({ ok: false });
  const game = getGame(key);
  game.word = word.trim().toLowerCase();
  game.active = true;
  game.winners.clear();
  io.to(`room:${key}`).emit('word:started', { word: game.word, winners: [] });
  res.json({ ok: true });
});

app.post('/api/word/stop', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getGame(key);
  game.active = false;
  io.to(`room:${key}`).emit('word:stopped', {});
  res.json({ ok: true });
});

app.post('/api/word/clear', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getGame(key);
  game.winners.clear();
  io.to(`room:${key}`).emit('word:update', { winners: [], count: 0 });
  res.json({ ok: true });
});

app.get('/api/word/:username', (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const game = getGame(key);
  res.json({
    word: game.word,
    active: game.active,
    winners: Array.from(game.winners.values()),
    count: game.winners.size,
  });
});

// ── Person Game Store (خمّن الشخصية) ─────────────────────
const personGames = {};

function getPersonGame(key) {
  if (!personGames[key]) personGames[key] = {
    person: null,
    active: false,
    currentHintLevel: 0,
    winners: [],
    playerStats: new Map(),
    pool: [],
    scale: 1,
    transitionDelay: 5000,
    pendingTransition: null,
    transitioning: false,
  };
  return personGames[key];
}

function schedulenextPerson(key, delay) {
  const game = getPersonGame(key);
  if (game.pendingTransition) clearTimeout(game.pendingTransition);
  game.pendingTransition = setTimeout(() => goToNextPerson(key), delay);
}

function goToNextPerson(key) {
  const game = getPersonGame(key);
  game.pendingTransition = null;
  game.transitioning = false;
  if (!game.pool.length) return;
  const prevName = game.person?.name;
  const pool = game.pool.filter(p => p.name !== prevName);
  if (!pool.length) return;
  const next = pool[Math.floor(Math.random() * pool.length)];
  game.person = next;
  game.active = true;
  game.currentHintLevel = 0;
  game.winners = [];
  io.to(`room:${key}`).emit('person:started', {
    image: next.image,
    blur: 20,
    hint: next.hints[0],
    hintIndex: 0,
    totalHints: next.hints.length,
    category: next.category,
    winners: [],
  });
  console.log(`[Person] Next: ${next.name}`);
}

app.post('/api/person/start', (req, res) => {
  const { username, person, pool } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  console.log(`[Person] /start called - key: ${key}, person: ${person?.name}, pool size: ${pool?.length}`);
  if (!key || !person) return res.json({ ok: false });
  const game = getPersonGame(key);
  if (game.pendingTransition) { clearTimeout(game.pendingTransition); game.pendingTransition = null; }
  game.transitioning = false;
  game.person = person;
  game.active = true;
  game.currentHintLevel = 0;
  game.winners = [];
  if (pool && Array.isArray(pool)) game.pool = pool;
  console.log(`[Person] Game state after start: active=${game.active}, person=${game.person?.name}, names=${[game.person?.name, ...(game.person?.aliases||[])].join(',')}`);
  io.to(`room:${key}`).emit('person:started', {
    image: person.image,
    blur: 20,
    hint: person.hints[0],
    hintIndex: 0,
    totalHints: person.hints.length,
    category: person.category,
    winners: [],
  });
  res.json({ ok: true });
});

app.post('/api/person/next-hint', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getPersonGame(key);
  if (!game.active || !game.person) return res.json({ ok: false });
  game.currentHintLevel = Math.min(game.currentHintLevel + 1, game.person.hints.length - 1);
  const blurValues = [20, 12, 6, 0];
  const blur = blurValues[Math.min(game.currentHintLevel, blurValues.length - 1)];
  io.to(`room:${key}`).emit('person:hint', {
    hint: game.person.hints[game.currentHintLevel],
    hintIndex: game.currentHintLevel,
    blur,
  });
  res.json({ ok: true, hintIndex: game.currentHintLevel });
});

app.post('/api/person/stop', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getPersonGame(key);
  if (game.pendingTransition) { clearTimeout(game.pendingTransition); game.pendingTransition = null; }
  game.active = false;
  game.transitioning = false;
  const top5 = Array.from(game.playerStats.entries())
    .map(([uid, s]) => ({ userId: uid, name: s.name, avatar: s.avatar, totalPersons: s.totalPersons }))
    .sort((a,b) => b.totalPersons - a.totalPersons)
    .slice(0, 5);
  io.to(`room:${key}`).emit('person:stopped', { answer: game.person?.name, top5 });
  res.json({ ok: true });
});

app.post('/api/person/reveal', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getPersonGame(key);
  if (!game.person) return res.json({ ok: false });
  game.active = false;
  io.to(`room:${key}`).emit('person:reveal', { answer: game.person.name, image: game.person.image });
  res.json({ ok: true });
});

app.post('/api/person/next', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getPersonGame(key);
  if (game.pendingTransition) { clearTimeout(game.pendingTransition); game.pendingTransition = null; }
  goToNextPerson(key);
  res.json({ ok: true });
});

app.post('/api/person/delay', (req, res) => {
  const { username, delay } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getPersonGame(key);
  game.transitionDelay = Math.max(0, Math.min(60000, parseInt(delay) || 5000));
  res.json({ ok: true, delay: game.transitionDelay });
});

app.post('/api/person/scale', (req, res) => {
  const { username, scale } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getPersonGame(key);
  game.scale = Math.max(0.6, Math.min(2, parseFloat(scale) || 1));
  io.to(`room:${key}`).emit('person:scale', { scale: game.scale });
  res.json({ ok: true, scale: game.scale });
});

app.post('/api/person/clear-stats', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  getPersonGame(key).playerStats.clear();
  res.json({ ok: true });
});

app.get('/api/person/:username', (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const game = getPersonGame(key);
  const allPlayers = Array.from(game.playerStats.entries())
    .map(([uid, s]) => ({ userId: uid, name: s.name, avatar: s.avatar, totalPersons: s.totalPersons }))
    .sort((a,b) => b.totalPersons - a.totalPersons);
  res.json({
    active: game.active,
    person: game.active ? {
      image: game.person?.image,
      category: game.person?.category,
      hint: game.person?.hints[game.currentHintLevel],
      hintIndex: game.currentHintLevel,
      totalHints: game.person?.hints.length,
    } : null,
    winners: game.winners,
    allPlayers,
    scale: game.scale || 1,
    transitionDelay: game.transitionDelay || 5000,
  });
});

// ── Countdown Game Store (العدّ التنازلي) ────────────────
const countdownGames = {};

function getCountdownGame(key) {
  if (!countdownGames[key]) countdownGames[key] = {
    question: '',
    answers: [], // قائمة إجابات صحيحة مطبعة
    originalAnswers: [], // الإجابات كما كتبها المقدم
    active: false,
    startTime: 0,
    endTime: 0,
    duration: 30,
    winners: [], // [{userId, name, avatar, answer, time}]
    recentAnswers: [], // آخر 10 إجابات
    timer: null,
  };
  return countdownGames[key];
}

app.post('/api/countdown/start', (req, res) => {
  const { username, question, answers, duration } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !question || !answers) return res.json({ ok: false });

  const game = getCountdownGame(key);
  if (game.timer) { clearTimeout(game.timer); game.timer = null; }

  // answers قد تكون string (مفصولة بفواصل) أو array
  const answersArr = Array.isArray(answers)
    ? answers
    : answers.split(/[,،]/g).map(s => s.trim()).filter(Boolean);

  game.question = question.trim();
  game.originalAnswers = answersArr;
  game.answers = answersArr.map(a => normalizePersonAnswer(a));
  game.active = true;
  game.duration = Math.max(5, Math.min(600, parseInt(duration) || 30));
  game.startTime = Date.now();
  game.endTime = game.startTime + game.duration * 1000;
  game.winners = [];
  game.recentAnswers = [];

  io.to(`room:${key}`).emit('countdown:started', {
    question: game.question,
    endTime: game.endTime,
    duration: game.duration,
  });

  // timer ينهي اللعبة تلقائياً
  game.timer = setTimeout(() => {
    game.active = false;
    io.to(`room:${key}`).emit('countdown:ended', {
      question: game.question,
      answers: game.originalAnswers,
      winners: game.winners.slice(0, 3),
      totalWinners: game.winners.length,
    });
  }, game.duration * 1000);

  console.log(`[Countdown] Started: "${question}" | answers: ${answersArr.join('|')}`);
  res.json({ ok: true });
});

app.post('/api/countdown/stop', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getCountdownGame(key);
  if (game.timer) { clearTimeout(game.timer); game.timer = null; }
  game.active = false;
  io.to(`room:${key}`).emit('countdown:ended', {
    question: game.question,
    answers: game.originalAnswers,
    winners: game.winners.slice(0, 3),
    totalWinners: game.winners.length,
  });
  res.json({ ok: true });
});

app.get('/api/countdown/:username', (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const game = getCountdownGame(key);
  res.json({
    active: game.active,
    question: game.question,
    endTime: game.endTime,
    duration: game.duration,
    winners: game.winners,
    recentAnswers: game.recentAnswers,
    totalWinners: game.winners.length,
  });
});

// ── Mystery Box Game Store (صناديق الحظ) ─────────────────
const mysteryGames = {};

function getMysteryGame(key) {
  if (!mysteryGames[key]) mysteryGames[key] = {
    boxes: [], // [{id, challenge, answers[], opened, winners[]}]
    currentBoxId: null,
    active: false,
    maxWinners: 3,
    duration: 30,
    boxEndTime: 0,
    boxTimer: null,
    playerStats: new Map(),
    recentAnswers: [],
  };
  return mysteryGames[key];
}

app.post('/api/mystery/setup', (req, res) => {
  const { username, boxes, maxWinners, duration } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !boxes || !Array.isArray(boxes) || !boxes.length) return res.json({ ok: false });

  const game = getMysteryGame(key);
  if (game.boxTimer) { clearTimeout(game.boxTimer); game.boxTimer = null; }

  game.boxes = boxes.map((b, i) => {
    const answersArr = Array.isArray(b.answers)
      ? b.answers
      : (b.answers || '').split(/[,،]/g).map(s => s.trim()).filter(Boolean);
    return {
      id: i + 1,
      challenge: b.challenge || '',
      answers: answersArr,
      answersClean: answersArr.map(a => normalizePersonAnswer(a)),
      opened: false,
      winners: [],
    };
  });
  game.maxWinners = Math.max(1, Math.min(10, parseInt(maxWinners) || 3));
  game.duration = Math.max(10, Math.min(300, parseInt(duration) || 30));
  game.currentBoxId = null;
  game.active = false;

  io.to(`room:${key}`).emit('mystery:setup', {
    totalBoxes: game.boxes.length,
    boxes: game.boxes.map(b => ({ id: b.id, opened: b.opened })),
  });
  res.json({ ok: true });
});

app.post('/api/mystery/open', (req, res) => {
  const { username, boxId } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getMysteryGame(key);
  if (!game.boxes.length) return res.json({ ok: false, message: 'لا توجد صناديق' });

  // ألغ timer سابق
  if (game.boxTimer) { clearTimeout(game.boxTimer); game.boxTimer = null; }

  // اختر صندوق محدد أو عشوائي من غير المفتوحة
  let box;
  if (boxId) {
    box = game.boxes.find(b => b.id === boxId && !b.opened);
  } else {
    const closed = game.boxes.filter(b => !b.opened);
    if (!closed.length) return res.json({ ok: false, message: 'كل الصناديق مفتوحة' });
    box = closed[Math.floor(Math.random() * closed.length)];
  }
  if (!box) return res.json({ ok: false });

  box.opened = true;
  box.winners = [];
  game.currentBoxId = box.id;
  game.active = true;
  game.recentAnswers = [];
  game.boxEndTime = Date.now() + game.duration * 1000;

  io.to(`room:${key}`).emit('mystery:opened', {
    boxId: box.id,
    challenge: box.challenge,
    endTime: game.boxEndTime,
    duration: game.duration,
    boxes: game.boxes.map(b => ({ id: b.id, opened: b.opened })),
    maxWinners: game.maxWinners,
  });

  // timer ينتهي تلقائياً
  game.boxTimer = setTimeout(() => {
    game.active = false;
    io.to(`room:${key}`).emit('mystery:timeUp', {
      boxId: box.id,
      challenge: box.challenge,
      answers: box.answers,
      winners: box.winners,
    });
  }, game.duration * 1000);

  console.log(`[Mystery] Opened box ${box.id}: "${box.challenge}"`);
  res.json({ ok: true, boxId: box.id });
});

app.post('/api/mystery/skip', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getMysteryGame(key);
  if (game.boxTimer) { clearTimeout(game.boxTimer); game.boxTimer = null; }
  game.active = false;
  io.to(`room:${key}`).emit('mystery:timeUp', {
    boxId: game.currentBoxId,
    challenge: '',
    answers: [],
    winners: [],
  });
  res.json({ ok: true });
});

app.post('/api/mystery/reset', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getMysteryGame(key);
  if (game.boxTimer) { clearTimeout(game.boxTimer); game.boxTimer = null; }
  game.active = false;
  game.boxes.forEach(b => { b.opened = false; b.winners = []; });
  game.currentBoxId = null;
  io.to(`room:${key}`).emit('mystery:setup', {
    totalBoxes: game.boxes.length,
    boxes: game.boxes.map(b => ({ id: b.id, opened: b.opened })),
  });
  res.json({ ok: true });
});

app.post('/api/mystery/clear-stats', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  getMysteryGame(key).playerStats.clear();
  res.json({ ok: true });
});

app.get('/api/mystery/:username', (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const game = getMysteryGame(key);
  const allPlayers = Array.from(game.playerStats.entries())
    .map(([uid, s]) => ({ userId: uid, name: s.name, avatar: s.avatar, totalWins: s.totalWins }))
    .sort((a,b) => b.totalWins - a.totalWins);
  const currentBox = game.currentBoxId
    ? game.boxes.find(b => b.id === game.currentBoxId)
    : null;
  res.json({
    active: game.active,
    boxes: game.boxes.map(b => ({ id: b.id, opened: b.opened })),
    currentBox: currentBox ? {
      id: currentBox.id,
      challenge: currentBox.challenge,
      winners: currentBox.winners,
    } : null,
    endTime: game.boxEndTime,
    maxWinners: game.maxWinners,
    allPlayers,
  });
});

// ── Photo Challenge Game Store (تحدي الصور) ────────────────────
const photoChallengeGames = {};

function getPhotoChallengeGame(key) {
  if (!photoChallengeGames[key]) photoChallengeGames[key] = {
    answer: '',        // الإجابة الصحيحة
    emojis: '',        // الإيموجي rebus (مثل 🐱🚂)
    hint: '',          // تلميح اختياري
    aliases: [],       // إجابات بديلة مقبولة
    questionIndex: 0,
    duration: 20,
    active: false,
    revealed: false,
    winner: null,
    winners: new Map(),
    endTime: 0,
    timer: null,
  };
  return photoChallengeGames[key];
}

app.post('/api/photo-challenge/start', (req, res) => {
  const { username, answer, emojis, image, hint, aliases, duration, questionIndex } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !answer) return res.json({ ok: false });

  const game = getPhotoChallengeGame(key);
  if (game.timer) { clearTimeout(game.timer); game.timer = null; }

  game.answer = answer;
  game.emojis = emojis || '❓';
  game.image = image || '';
  game.hint = hint || '';
  game.aliases = Array.isArray(aliases) ? aliases : [];
  game.questionIndex = questionIndex || 0;
  game.duration = Math.max(5, Math.min(60, parseInt(duration) || 20));
  game.active = true;
  game.revealed = false;
  game.winner = null;
  game.endTime = Date.now() + game.duration * 1000;

  io.to(`room:${key}`).emit('photo-challenge:question', {
    emojis: game.emojis,
    image: game.image,
    hint: game.hint,
    duration: game.duration,
    questionIndex: game.questionIndex,
  });

  console.log(`[PhotoChallenge] Q#${game.questionIndex}: ${game.answer} ${game.emojis}`);
  res.json({ ok: true });
});

app.post('/api/photo-challenge/reveal', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getPhotoChallengeGame(key);
  if (game.timer) { clearTimeout(game.timer); game.timer = null; }
  game.revealed = true;
  game.active = false;
  io.to(`room:${key}`).emit('photo-challenge:reveal', { answer: game.answer });
  res.json({ ok: true });
});

app.post('/api/photo-challenge/hint', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getPhotoChallengeGame(key);
  io.to(`room:${key}`).emit('photo-challenge:hint', { hint: game.hint || '' });
  console.log(`[PhotoChallenge] Hint: ${game.hint}`);
  res.json({ ok: true });
});

app.post('/api/photo-challenge/stop', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getPhotoChallengeGame(key);
  if (game.timer) { clearTimeout(game.timer); game.timer = null; }
  game.active = false;
  game.revealed = false;
  io.to(`room:${key}`).emit('photo-challenge:stopped');
  res.json({ ok: true });
});

app.post('/api/photo-challenge/clear-stats', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  getPhotoChallengeGame(key).winners.clear();
  res.json({ ok: true });
});

app.get('/api/photo-challenge/:username', (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const game = getPhotoChallengeGame(key);
  const allWinners = Array.from(game.winners.entries())
    .map(([uid, w]) => ({ userId: uid, ...w }))
    .sort((a,b) => b.totalWins - a.totalWins);
  res.json({
    active: game.active,
    revealed: game.revealed,
    answer: game.revealed ? game.answer : null,
    emojis: game.emojis,
    image: game.image,
    hint: game.hint,
    questionIndex: game.questionIndex,
    duration: game.duration,
    endTime: game.endTime,
    winner: game.winner,
    allWinners,
  });
});

// ── REST API ──────────────────────────────────────────────

app.post('/api/connect', async (req, res) => {
  const { username, sessionid } = req.body;
  if (!username) return res.json({ ok: false, message: 'username required' });
  const key = username.toLowerCase().replace('@', '').trim();
  connectRoom(key, sessionid || null);
  res.json({ ok: true, username: key });
});

app.post('/api/disconnect', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@', '').trim();
  const room = rooms[key];
  if (!room) return res.json({ ok: false });
  // Mark as removed FIRST to prevent retry
  room.status = 'removed';
  if (room.retryTimer) clearTimeout(room.retryTimer);
  // Remove all event listeners before disconnecting
  if (room.tiktok) {
    try {
      room.tiktok.removeAllListeners();
      room.tiktok.disconnect();
    } catch (_) {}
  }
  delete rooms[key];
  io.emit('room:status', { username: key, status: 'removed' });
  console.log(`[TikTok] Permanently disconnected @${key}`);
  res.json({ ok: true });
});

app.get('/api/rooms', (req, res) => {
  res.json(Object.entries(rooms).map(([username, room]) => ({
    username,
    status: room.status,
    stats: room.stats,
    msgCount: room.messages.length,
    eventCounts: room.eventCounts || {},
  })));
});

// ── Socket.IO ─────────────────────────────────────────────

io.on('connection', (socket) => {
  socket.on('join', ({ username }) => {
    const key = username?.toLowerCase().replace('@', '').trim();
    if (!key) return;

    // Leave previous room if any to avoid double messages
    socket.rooms.forEach(room => {
      if (room.startsWith('room:') && room !== `room:${key}`) {
        socket.leave(room);
      }
    });

    socket.join(`room:${key}`);

    // Relay slot color changes to overlays
    socket.on('slot:color', (data) => {
      const k = data.username?.toLowerCase().replace('@','').trim();
      if (k) io.to(`room:${k}`).emit('slot:color', data);
    });

    // Relay word-war reg visible count
    socket.on('word-war:reg-visible', (data) => {
      const k = data.username?.toLowerCase().replace('@','').trim();
      if (k) io.to(`room:${k}`).emit('word-war:reg-visible', data);
    });

    // Relay drawing strokes
    socket.on('draw:stroke', (data) => {
      const k = data.username?.toLowerCase().replace('@','').trim();
      if (k) socket.to(`room:${k}`).emit('draw:stroke', data);
    });
    socket.on('draw:clear-canvas', (data) => {
      const k = data.username?.toLowerCase().replace('@','').trim();
      if (k) socket.to(`room:${k}`).emit('draw:clear-canvas');
    });

    const room = rooms[key];
    if (room) {
      socket.emit('stats', room.stats);
      socket.emit('history', room.messages.slice(-30));
      socket.emit('room:status', { username: key, status: room.status });
      // Send wheel data
      const wheel = getWheel(key);
      socket.emit('wheel:update', {
        entries: Array.from(wheel.entries.values()),
        count: wheel.entries.size,
        keyword: wheel.keyword,
      });
      // Send guess game data
      const guessGame = getGuessGame(key);
      if (guessGame.active) {
        socket.emit('guess:started', {
          length: guessGame.word.length, hint: guessGame.hint,
          revealed: guessGame.revealed,
          letters: guessGame.revealed.map(i => ({ i, c: guessGame.word[i] })),
        });
      }
      // Send word game data
      const game = getGame(key);
      socket.emit('word:update', {
        word: game.word,
        active: game.active,
        winners: Array.from(game.winners.values()),
        count: game.winners.size,
      });
    } else {
      connectRoom(key);
    }
  });

  socket.on('leave', ({ username }) => {
    const key = username?.toLowerCase().replace('@', '').trim();
    if (key) socket.leave(`room:${key}`);
  });
});

const VERSION = 'v2.1.0-euler';
const PORT = process.env.PORT || 3000;
// ── شبكات أمان على مستوى العملية ─────────────────────────
// أي خطأ غير معالَج (مثل فشل مصافحة WebSocket بـ 522 من Cloudflare)
// يُسجَّل ولا يُسقط السيرفر — الغرف والألعاب تبقى حية
process.on('uncaughtException', (err) => {
  console.error(`[FATAL-CAUGHT] uncaughtException: ${err?.message || err}`);
  if (err?.stack) console.error(err.stack.split('\n').slice(0, 4).join('\n'));
});
process.on('unhandledRejection', (reason) => {
  console.error(`[FATAL-CAUGHT] unhandledRejection: ${reason?.message || reason}`);
});

httpServer.listen(PORT, () => {
  console.log(`\n🎉 فعاليات تيك توك ${VERSION} running at http://localhost:${PORT}\n`);
});

// Version API
// ── شروط اللعب (بطاقات كوميك تظهر بالأوفرلي قبل بدء الفعالية) ──
app.post('/api/rules/show', (req, res) => {
  const { username, game, title, rules, duration } = req.body;
  const key = username?.toLowerCase().replace('@', '').trim();
  if (!key || !Array.isArray(rules) || !rules.length) return res.json({ ok: false, message: 'username والشروط مطلوبة' });
  broadcast(key, 'rules:show', {
    game: String(game || ''),
    title: String(title || 'شروط اللعب'),
    rules: rules.map(r => String(r)).filter(Boolean).slice(0, 8),
    duration: Math.max(0, Math.min(120, parseInt(duration) || 0)),
  });
  res.json({ ok: true });
});
app.post('/api/rules/hide', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@', '').trim();
  if (!key) return res.json({ ok: false });
  broadcast(key, 'rules:hide', {});
  res.json({ ok: true });
});

app.get('/api/version', (req, res) => res.json({ version: VERSION }));

// ══════════════════════════════════════════════════════════
// ── Secret Word Game (كلمة السر) ─────────────────────────
// ══════════════════════════════════════════════════════════
const secretWordGames = {};
function getSecretWordGame(key) {
  if (!secretWordGames[key]) secretWordGames[key] = {
    word: '', letters: [], revealedIndices: [], duration: 30, revealInterval: 4,
    active: false, revealed: false, winner: null, endTime: 0,
    revealTimer: null, endTimer: null, questionIndex: 0,
    aliases: [], hint: '', category: '',
    winners: new Map(),
  };
  return secretWordGames[key];
}

app.post('/api/secret-word/start', (req, res) => {
  const { username, word, aliases, hint, category, duration, revealInterval, questionIndex } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !word) return res.json({ ok: false });

  const game = getSecretWordGame(key);
  // clear old timers
  if (game.revealTimer) clearInterval(game.revealTimer);
  if (game.endTimer) clearTimeout(game.endTimer);

  game.word = word;
  game.letters = word.split('');
  game.revealedIndices = [];
  game.duration = Math.max(10, Math.min(120, parseInt(duration) || 30));
  game.revealInterval = Math.max(1, Math.min(15, parseInt(revealInterval) || 4));
  game.active = true;
  game.revealed = false;
  game.winner = null;
  game.aliases = Array.isArray(aliases) ? aliases : [];
  game.hint = hint || '';
  game.category = category || '';
  game.questionIndex = questionIndex || 0;
  game.endTime = Date.now() + game.duration * 1000;

  // Send initial question (all hidden)
  io.to(`room:${key}`).emit('secret-word:question', {
    length: game.letters.length,
    revealedIndices: [],
    letters: [],
    hint: game.hint,
    category: game.category,
    duration: game.duration,
    questionIndex: game.questionIndex,
  });

  // Schedule letter reveals
  const totalLetters = game.letters.length;
  let revealCount = 0;
  game.revealTimer = setInterval(() => {
    if (!game.active || game.revealed) { clearInterval(game.revealTimer); return; }
    // pick a random unrevealed index
    const unrevealed = [];
    for (let i = 0; i < totalLetters; i++) {
      if (!game.revealedIndices.includes(i)) unrevealed.push(i);
    }
    // keep at least 1 letter hidden (so people can still guess)
    if (unrevealed.length <= 1) { clearInterval(game.revealTimer); return; }
    const pick = unrevealed[Math.floor(Math.random() * unrevealed.length)];
    game.revealedIndices.push(pick);
    revealCount++;

    const lettersToSend = game.letters.map((l, i) => game.revealedIndices.includes(i) ? l : null);
    io.to(`room:${key}`).emit('secret-word:reveal-letter', {
      revealedIndices: [...game.revealedIndices],
      letters: lettersToSend,
    });
    console.log(`[SecretWord] Revealed letter #${revealCount}: "${game.letters[pick]}" at index ${pick}`);
  }, game.revealInterval * 1000);

  // Schedule auto-reveal at end
  game.endTimer = setTimeout(() => {
    if (!game.active) return;
    if (game.revealTimer) clearInterval(game.revealTimer);
    game.revealed = true;
    game.active = false;
    io.to(`room:${key}`).emit('secret-word:reveal', { word: game.word });
    console.log(`[SecretWord] Time up! Word was: ${game.word}`);
  }, game.duration * 1000);

  console.log(`[SecretWord] Q#${game.questionIndex}: "${game.word}" (${game.letters.length} letters, reveal every ${game.revealInterval}s)`);
  res.json({ ok: true });
});

app.post('/api/secret-word/reveal', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getSecretWordGame(key);
  if (game.revealTimer) clearInterval(game.revealTimer);
  if (game.endTimer) clearTimeout(game.endTimer);
  game.revealed = true;
  game.active = false;
  io.to(`room:${key}`).emit('secret-word:reveal', { word: game.word });
  res.json({ ok: true });
});

app.post('/api/secret-word/hint', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getSecretWordGame(key);
  io.to(`room:${key}`).emit('secret-word:hint', { hint: game.hint || '' });
  res.json({ ok: true });
});

app.post('/api/secret-word/stop', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getSecretWordGame(key);
  if (game.revealTimer) clearInterval(game.revealTimer);
  if (game.endTimer) clearTimeout(game.endTimer);
  game.active = false;
  game.revealed = false;
  io.to(`room:${key}`).emit('secret-word:stopped');
  res.json({ ok: true });
});

app.post('/api/secret-word/clear-stats', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  getSecretWordGame(key).winners.clear();
  res.json({ ok: true });
});

app.get('/api/secret-word/:username', (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const game = getSecretWordGame(key);
  const allWinners = Array.from(game.winners.entries())
    .map(([uid, w]) => ({ userId: uid, ...w }))
    .sort((a,b) => b.totalWins - a.totalWins);
  const lettersToSend = game.letters.map((l, i) => game.revealedIndices.includes(i) ? l : null);
  res.json({
    active: game.active,
    revealed: game.revealed,
    word: game.revealed ? game.word : null,
    length: game.letters.length,
    revealedIndices: game.revealedIndices,
    letters: lettersToSend,
    hint: game.hint,
    category: game.category,
    duration: game.duration,
    endTime: game.endTime,
    winner: game.winner,
    allWinners,
  });
});

// ══════════════════════════════════════════════════════════
// ── Word War Game (حرب الكلمات) ──────────────────────────
// ══════════════════════════════════════════════════════════
const wordWarGames = {};
function getWordWarGame(key) {
  if (!wordWarGames[key]) wordWarGames[key] = {
    category: '', validWords: [], validWordsSet: new Set(), duration: 60,
    active: false, endTime: 0, endTimer: null,
    redTeam: new Map(), blueTeam: new Map(), // userId -> {name, avatar, words:[], totalWords:0}
    redWords: new Set(), blueWords: new Set(),
    redScore: 0, blueScore: 0,
    roundHistory: [],
    registrationOpen: false, registrationLocked: false,
    redKeyword: 'أحمر', blueKeyword: 'أزرق',
  };
  return wordWarGames[key];
}

app.post('/api/word-war/start', (req, res) => {
  const { username, category, validWords, duration, redKeyword, blueKeyword, resetTeams } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !category) return res.json({ ok: false });

  const game = getWordWarGame(key);
  if (game.endTimer) clearTimeout(game.endTimer);

  game.category = category;
  game.validWords = (validWords || []).map(w => w.trim().toLowerCase().replace(/\s+/g,''));
  game.validWordsSet = new Set(game.validWords);
  game.duration = Math.max(15, Math.min(120, parseInt(duration) || 60));
  game.active = true;
  game.endTime = Date.now() + game.duration * 1000;
  // Only clear teams if resetTeams is true (new round)
  if (resetTeams) {
    game.redTeam.clear(); game.blueTeam.clear();
    game.redScore = 0; game.blueScore = 0;
  }
  // Always clear category words for new category
  game.redWords.clear(); game.blueWords.clear();
  // Reset per-category words but keep totalWords (only reset on new round)
  for (const [uid, p] of game.redTeam) {
    p.words = [];
    if (resetTeams) p.totalWords = 0;
  }
  for (const [uid, p] of game.blueTeam) {
    p.words = [];
    if (resetTeams) p.totalWords = 0;
  }

  game.redKeyword = (redKeyword || 'أحمر').trim().toLowerCase().replace(/\s+/g,'');
  game.blueKeyword = (blueKeyword || 'أزرق').trim().toLowerCase().replace(/\s+/g,'');
  if (resetTeams) game.registrationLocked = false;

  io.to(`room:${key}`).emit('word-war:start', {
    category: game.category,
    duration: game.duration,
    redKeyword: redKeyword || 'أحمر',
    blueKeyword: blueKeyword || 'أزرق',
    redCount: game.redTeam.size,
    blueCount: game.blueTeam.size,
    redScore: game.redScore,
    blueScore: game.blueScore,
    resetTeams: !!resetTeams,
  });

  game.endTimer = setTimeout(() => {
    if (!game.active) return;
    game.active = false;
    const result = {
      category: game.category,
      redScore: game.redScore, blueScore: game.blueScore,
      winner: game.redScore > game.blueScore ? 'red' : game.blueScore > game.redScore ? 'blue' : 'tie',
      redWords: [...game.redWords], blueWords: [...game.blueWords],
    };
    game.roundHistory.push(result);
    io.to(`room:${key}`).emit('word-war:end', result);
    console.log(`[WordWar] Round end: Red ${game.redScore} - Blue ${game.blueScore}`);
  }, game.duration * 1000);

  console.log(`[WordWar] Started: "${category}" (${game.validWords.length} words, ${game.duration}s, teams ${resetTeams ? 'RESET' : 'KEPT'}: R${game.redTeam.size} B${game.blueTeam.size})`);
  res.json({ ok: true });
});

app.post('/api/word-war/open-registration', (req, res) => {
  const { username, redKeyword, blueKeyword } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getWordWarGame(key);
  game.redKeyword = (redKeyword || 'أحمر').trim().toLowerCase().replace(/\s+/g,'');
  game.blueKeyword = (blueKeyword || 'أزرق').trim().toLowerCase().replace(/\s+/g,'');
  game.registrationLocked = false;
  game.registrationOpen = true;
  // DON'T clear teams — only new round clears teams
  io.to(`room:${key}`).emit('word-war:registration', { open: true, redKeyword: redKeyword || 'أحمر', blueKeyword: blueKeyword || 'أزرق', redCount: game.redTeam.size, blueCount: game.blueTeam.size });
  console.log(`[WordWar] Registration OPENED (Red: "${redKeyword}", Blue: "${blueKeyword}") — Teams kept: R${game.redTeam.size} B${game.blueTeam.size}`);
  res.json({ ok: true });
});

app.post('/api/word-war/lock', (req, res) => {
  const { username, locked } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getWordWarGame(key);
  game.registrationLocked = !!locked;
  io.to(`room:${key}`).emit('word-war:lock', { locked: game.registrationLocked });
  console.log(`[WordWar] Registration ${locked ? 'LOCKED' : 'UNLOCKED'}`);
  res.json({ ok: true });
});

app.post('/api/word-war/remove-player', (req, res) => {
  const { username, team, playerName } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getWordWarGame(key);
  const teamMap = team === 'red' ? game.redTeam : game.blueTeam;
  for (const [uid, p] of teamMap) {
    if (p.name === playerName) {
      teamMap.delete(uid);
      console.log(`[WordWar] Removed ${playerName} from ${team}`);
      io.to(`room:${key}`).emit('word-war:player-removed', { team, playerName, redCount: game.redTeam.size, blueCount: game.blueTeam.size });
      break;
    }
  }
  res.json({ ok: true });
});

app.post('/api/word-war/add-player', (req, res) => {
  const { username, team, playerName } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !playerName) return res.json({ ok: false });
  const game = getWordWarGame(key);
  const teamMap = team === 'red' ? game.redTeam : game.blueTeam;
  const uid = 'manual_' + Date.now() + '_' + Math.random().toString(36).slice(2,6);
  teamMap.set(uid, { name: playerName.trim(), avatar: null, words: [], totalWords: 0 });
  io.to(`room:${key}`).emit('word-war:join', { team, player: playerName.trim(), avatar: null, redCount: game.redTeam.size, blueCount: game.blueTeam.size });
  console.log(`[WordWar] Manual add: ${playerName} to ${team}`);
  res.json({ ok: true });
});

app.post('/api/word-war/clear-teams', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getWordWarGame(key);
  game.redTeam.clear(); game.blueTeam.clear();
  game.redWords.clear(); game.blueWords.clear();
  game.redScore = 0; game.blueScore = 0;
  io.to(`room:${key}`).emit('word-war:teams-cleared');
  console.log(`[WordWar] Teams cleared`);
  res.json({ ok: true });
});

app.post('/api/word-war/stop', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getWordWarGame(key);
  if (game.endTimer) clearTimeout(game.endTimer);
  game.active = false;
  io.to(`room:${key}`).emit('word-war:stopped');
  res.json({ ok: true });
});

app.post('/api/word-war/clear', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getWordWarGame(key);
  game.roundHistory = [];
  res.json({ ok: true });
});

app.get('/api/word-war/:username', (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const game = getWordWarGame(key);
  const redPlayers = Array.from(game.redTeam.entries()).map(([uid, p]) => ({ userId: uid, ...p, totalWords: p.totalWords || 0 })).sort((a,b) => (b.totalWords||0) - (a.totalWords||0));
  const bluePlayers = Array.from(game.blueTeam.entries()).map(([uid, p]) => ({ userId: uid, ...p, totalWords: p.totalWords || 0 })).sort((a,b) => (b.totalWords||0) - (a.totalWords||0));
  res.json({
    active: game.active,
    category: game.category,
    duration: game.duration,
    endTime: game.endTime,
    redScore: game.redScore, blueScore: game.blueScore,
    redWords: [...game.redWords], blueWords: [...game.blueWords],
    redPlayers, bluePlayers,
    roundHistory: game.roundHistory.slice(-10),
    registrationOpen: game.registrationOpen || false,
    registrationLocked: game.registrationLocked || false,
    redKeyword: game.redKeyword || '',
    blueKeyword: game.blueKeyword || '',
  });
});

// ══════════════════════════════════════════════════════════
// ── Memory Challenge (تحدي الذاكرة) ─────────────────────
// ══════════════════════════════════════════════════════════
const memoryGames = {};
function getMemoryGame(key) {
  if (!memoryGames[key]) memoryGames[key] = {
    active: false, sequence: '', round: 0, difficulty: 4,
    showTime: 3, players: new Map(), // userId -> {name, avatar, alive, score}
    eliminated: [], winner: null, phase: 'idle', // idle, register, showing, answering
    phaseTimer: null, answerTime: 10,
  };
  return memoryGames[key];
}

function generateSequence(length, type) {
  const chars = type === 'numbers' ? '0123456789' : type === 'letters' ? 'ابتثجحخدذرزسشصضطظعغفقكلمنهوي' : '0123456789ابتثجحخد';
  let seq = '';
  for (let i = 0; i < length; i++) seq += chars[Math.floor(Math.random() * chars.length)];
  return seq;
}

app.post('/api/memory/start-register', (req, res) => {
  const { username, keyword } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getMemoryGame(key);
  if (game.phaseTimer) clearTimeout(game.phaseTimer);
  game.players.clear();
  game.eliminated = [];
  game.winner = null;
  game.round = 0;
  game.phase = 'register';
  game.keyword = (keyword || 'ذاكرة').toLowerCase().replace(/\s+/g,'');
  game.active = true;
  io.to(`room:${key}`).emit('memory:register', { keyword: keyword || 'ذاكرة' });
  console.log(`[Memory] Registration opened for @${key}`);
  res.json({ ok: true });
});

app.post('/api/memory/start-round', (req, res) => {
  const { username, difficulty, showTime, answerTime, seqType } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getMemoryGame(key);
  if (game.phaseTimer) clearTimeout(game.phaseTimer);

  game.round++;
  game.difficulty = parseInt(difficulty) || (3 + game.round);
  game.showTime = Math.max(1, parseFloat(showTime) || Math.max(1.5, 4 - game.round * 0.3));
  game.answerTime = parseInt(answerTime) || 10;
  game.sequence = generateSequence(game.difficulty, seqType || 'numbers');
  game.phase = 'showing';
  game.roundAnswers = new Set();

  // Send sequence to overlay (show phase)
  io.to(`room:${key}`).emit('memory:show', {
    sequence: game.sequence,
    round: game.round,
    showTime: game.showTime,
    difficulty: game.difficulty,
    alivePlayers: Array.from(game.players.values()).filter(p => p.alive).length,
  });

  // After showTime, switch to answer phase
  game.phaseTimer = setTimeout(() => {
    game.phase = 'answering';
    io.to(`room:${key}`).emit('memory:answer', {
      round: game.round,
      answerTime: game.answerTime,
      length: game.sequence.length,
    });

    // After answerTime, eliminate non-answerers
    game.phaseTimer = setTimeout(() => {
      game.phase = 'results';
      // Eliminate players who didn't answer
      for (const [uid, p] of game.players) {
        if (p.alive && !game.roundAnswers.has(uid)) {
          p.alive = false;
          game.eliminated.push({ name: p.name, avatar: p.avatar, round: game.round, reason: 'لم يجاوب' });
        }
      }
      const alive = Array.from(game.players.values()).filter(p => p.alive);
      let winner = null;
      if (alive.length <= 1) {
        winner = alive.length === 1 ? alive[0] : null;
        game.winner = winner;
        game.active = false;
      }
      io.to(`room:${key}`).emit('memory:results', {
        round: game.round,
        sequence: game.sequence,
        alive: alive.map(p => ({ name: p.name, avatar: p.avatar, score: p.score })),
        eliminated: game.eliminated.slice(-5),
        winner: winner ? { name: winner.name, avatar: winner.avatar, score: winner.score } : null,
        totalAlive: alive.length,
      });
      console.log(`[Memory] Round ${game.round} done: ${alive.length} alive, seq="${game.sequence}"`);
    }, game.answerTime * 1000);
  }, game.showTime * 1000);

  console.log(`[Memory] Round ${game.round}: "${game.sequence}" (show ${game.showTime}s, answer ${game.answerTime}s)`);
  res.json({ ok: true, sequence: game.sequence, round: game.round });
});

app.post('/api/memory/stop', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getMemoryGame(key);
  if (game.phaseTimer) clearTimeout(game.phaseTimer);
  game.active = false;
  game.phase = 'idle';
  io.to(`room:${key}`).emit('memory:stopped');
  res.json({ ok: true });
});

app.get('/api/memory/:username', (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const game = getMemoryGame(key);
  const players = Array.from(game.players.values());
  res.json({
    active: game.active, phase: game.phase, round: game.round,
    players: players.sort((a,b) => b.score - a.score),
    alivePlayers: players.filter(p => p.alive).length,
    totalPlayers: players.length,
    eliminated: game.eliminated.slice(-10),
    winner: game.winner,
  });
});

// ══════════════════════════════════════════════════════════
// ── Millionaire Game (من سيربح المليون) ──────────────────
// ══════════════════════════════════════════════════════════
const millionaireGames = {};
function getMillionaireGame(key) {
  if (!millionaireGames[key]) millionaireGames[key] = {
    active: false, phase: 'idle', // idle, question, results
    currentQ: null, correctAnswer: 0, questionIndex: 0,
    answers: new Map(), // userId -> { name, avatar, answer }
    scores: new Map(), // userId -> { name, avatar, totalScore, streak }
    answerTime: 15, phaseTimer: null,
    helps: { remove2: true, audience: true, change: true },
  };
  return millionaireGames[key];
}

const MILLION_POINTS = [100,200,300,500,1000,2000,4000,8000,16000,32000,64000,125000,250000,500000,1000000];

app.post('/api/million/ask', (req, res) => {
  const { username, question, options, correct, answerTime, questionIndex } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !question || !options) return res.json({ ok: false });
  const game = getMillionaireGame(key);
  if (game.phaseTimer) clearTimeout(game.phaseTimer);

  game.active = true;
  game.phase = 'question';
  game.currentQ = question;
  game.correctAnswer = parseInt(correct) || 1;
  game.questionIndex = parseInt(questionIndex) || 0;
  game.answerTime = parseInt(answerTime) || 15;
  game.answers.clear();
  game.visibleOptions = options;

  io.to(`room:${key}`).emit('million:question', {
    question, options, answerTime: game.answerTime,
    questionIndex: game.questionIndex,
    points: MILLION_POINTS[Math.min(game.questionIndex, MILLION_POINTS.length - 1)],
  });

  // Auto-reveal after answer time
  game.phaseTimer = setTimeout(() => {
    revealMillionAnswer(key);
  }, game.answerTime * 1000);

  console.log(`[Million] Q${game.questionIndex + 1}: "${question}" correct=${correct}`);
  res.json({ ok: true });
});

function revealMillionAnswer(key) {
  const game = getMillionaireGame(key);
  if (game.phase !== 'question') return;
  game.phase = 'results';

  const correct = game.correctAnswer;
  let correctCount = 0, wrongCount = 0;
  const points = MILLION_POINTS[Math.min(game.questionIndex, MILLION_POINTS.length - 1)];

  for (const [uid, a] of game.answers) {
    const isCorrect = a.answer === correct;
    if (isCorrect) {
      correctCount++;
      const s = game.scores.get(uid) || { name: a.name, avatar: a.avatar, totalScore: 0, streak: 0, correct: 0 };
      s.totalScore += points;
      s.streak++;
      s.correct++;
      s.name = a.name;
      s.avatar = a.avatar;
      game.scores.set(uid, s);
    } else {
      wrongCount++;
      const s = game.scores.get(uid);
      if (s) s.streak = 0;
    }
  }

  const topPlayers = Array.from(game.scores.values()).sort((a, b) => b.totalScore - a.totalScore).slice(0, 5);
  const answerDist = [0, 0, 0, 0];
  for (const [uid, a] of game.answers) {
    if (a.answer >= 1 && a.answer <= 4) answerDist[a.answer - 1]++;
  }

  io.to(`room:${key}`).emit('million:reveal', {
    correct, correctCount, wrongCount,
    totalAnswers: game.answers.size,
    answerDist, points,
    topPlayers, questionIndex: game.questionIndex,
  });
  console.log(`[Million] Reveal: correct=${correctCount} wrong=${wrongCount}`);
}

app.post('/api/million/reveal', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getMillionaireGame(key);
  if (game.phaseTimer) clearTimeout(game.phaseTimer);
  revealMillionAnswer(key);
  res.json({ ok: true });
});

app.post('/api/million/help', (req, res) => {
  const { username, type } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getMillionaireGame(key);

  if (type === 'remove2' && game.helps.remove2) {
    game.helps.remove2 = false;
    // Remove 2 wrong options
    const wrong = [1, 2, 3, 4].filter(i => i !== game.correctAnswer);
    const removed = wrong.sort(() => Math.random() - 0.5).slice(0, 2);
    io.to(`room:${key}`).emit('million:help', { type: 'remove2', removed });
  } else if (type === 'audience' && game.helps.audience) {
    game.helps.audience = false;
    // Fake audience vote (biased toward correct)
    const dist = [0, 0, 0, 0];
    for (let i = 0; i < 100; i++) {
      if (Math.random() < 0.6) dist[game.correctAnswer - 1]++;
      else dist[Math.floor(Math.random() * 4)]++;
    }
    io.to(`room:${key}`).emit('million:help', { type: 'audience', dist });
  } else if (type === 'change' && game.helps.change) {
    game.helps.change = false;
    io.to(`room:${key}`).emit('million:help', { type: 'change' });
  }
  res.json({ ok: true, helps: game.helps });
});

app.post('/api/million/stop', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getMillionaireGame(key);
  if (game.phaseTimer) clearTimeout(game.phaseTimer);
  game.active = false; game.phase = 'idle';
  io.to(`room:${key}`).emit('million:stopped');
  res.json({ ok: true });
});

app.post('/api/million/reset', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getMillionaireGame(key);
  game.scores.clear();
  game.questionIndex = 0;
  game.helps = { remove2: true, audience: true, change: true };
  res.json({ ok: true });
});

app.get('/api/million/:username', (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const game = getMillionaireGame(key);
  const topPlayers = Array.from(game.scores.values()).sort((a, b) => b.totalScore - a.totalScore);
  res.json({ active: game.active, phase: game.phase, questionIndex: game.questionIndex, helps: game.helps, topPlayers });
});

// ══════════════════════════════════════════════════════════
// ── Knockout Tournament (بطولة خروج) ────────────────────
// ══════════════════════════════════════════════════════════
const knockoutGames = {};
function getKnockoutGame(key) {
  if (!knockoutGames[key]) knockoutGames[key] = {
    active: false, phase: 'idle', // idle, register, question, results, finished
    players: new Map(), // userId -> {name, avatar, alive}
    eliminated: [], round: 0, maxPlayers: 16,
    currentQ: null, correctAnswer: 0, answerTime: 15,
    answers: new Map(), phaseTimer: null,
    keyword: 'بطولة', registrationLocked: false,
  };
  return knockoutGames[key];
}

app.post('/api/knockout/register', (req, res) => {
  const { username, keyword, maxPlayers } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getKnockoutGame(key);
  if (game.phaseTimer) clearTimeout(game.phaseTimer);
  game.players.clear(); game.eliminated = []; game.round = 0;
  game.phase = 'register'; game.active = true;
  game.keyword = (keyword || 'بطولة').toLowerCase().replace(/\s+/g,'');
  game.maxPlayers = parseInt(maxPlayers) || 16;
  game.registrationLocked = false;
  io.to(`room:${key}`).emit('knockout:register', { keyword: keyword || 'بطولة', maxPlayers: game.maxPlayers });
  console.log(`[Knockout] Registration opened (max ${game.maxPlayers})`);
  res.json({ ok: true });
});

app.post('/api/knockout/lock', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getKnockoutGame(key);
  game.registrationLocked = true;
  game.phase = 'locked';
  io.to(`room:${key}`).emit('knockout:locked', { players: Array.from(game.players.values()), count: game.players.size });
  res.json({ ok: true });
});

app.post('/api/knockout/ask', (req, res) => {
  const { username, question, options, correct, answerTime } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !question) return res.json({ ok: false });
  const game = getKnockoutGame(key);
  if (game.phaseTimer) clearTimeout(game.phaseTimer);
  game.round++;
  game.phase = 'question';
  game.currentQ = question;
  game.correctAnswer = parseInt(correct) || 1;
  game.answerTime = parseInt(answerTime) || 15;
  game.answers.clear();
  const alive = Array.from(game.players.values()).filter(p => p.alive);
  io.to(`room:${key}`).emit('knockout:question', {
    question, options, answerTime: game.answerTime, round: game.round,
    alivePlayers: alive.length,
  });
  // Auto reveal
  game.phaseTimer = setTimeout(() => revealKnockout(key), game.answerTime * 1000);
  console.log(`[Knockout] Round ${game.round}: "${question}" (${alive.length} alive)`);
  res.json({ ok: true });
});

function revealKnockout(key) {
  const game = getKnockoutGame(key);
  if (game.phase !== 'question') return;
  game.phase = 'results';
  const correct = game.correctAnswer;
  const roundEliminated = [];
  // Check answers + eliminate wrong/no-answer
  for (const [uid, p] of game.players) {
    if (!p.alive) continue;
    const ans = game.answers.get(uid);
    if (!ans || ans.answer !== correct) {
      p.alive = false;
      roundEliminated.push({ name: p.name, avatar: p.avatar, answer: ans ? ans.answer : 0 });
      game.eliminated.push({ name: p.name, avatar: p.avatar, round: game.round });
    }
  }
  const alive = Array.from(game.players.values()).filter(p => p.alive);
  let winner = null;
  if (alive.length <= 1) {
    winner = alive.length === 1 ? alive[0] : null;
    game.phase = 'finished';
    game.active = false;
  }
  io.to(`room:${key}`).emit('knockout:reveal', {
    correct, round: game.round,
    eliminated: roundEliminated,
    alive: alive.map(p => ({ name: p.name, avatar: p.avatar })),
    aliveCount: alive.length,
    winner: winner ? { name: winner.name, avatar: winner.avatar } : null,
  });
  console.log(`[Knockout] Round ${game.round}: eliminated ${roundEliminated.length}, alive ${alive.length}`);
}

app.post('/api/knockout/reveal', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getKnockoutGame(key);
  if (game.phaseTimer) clearTimeout(game.phaseTimer);
  revealKnockout(key);
  res.json({ ok: true });
});

app.post('/api/knockout/stop', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getKnockoutGame(key);
  if (game.phaseTimer) clearTimeout(game.phaseTimer);
  game.active = false; game.phase = 'idle';
  io.to(`room:${key}`).emit('knockout:stopped');
  res.json({ ok: true });
});

app.get('/api/knockout/:username', (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const game = getKnockoutGame(key);
  const players = Array.from(game.players.values());
  res.json({
    active: game.active, phase: game.phase, round: game.round,
    players, aliveCount: players.filter(p => p.alive).length,
    totalPlayers: players.length, eliminated: game.eliminated,
  });
});

// ══════════════════════════════════════════════════════════
// ── Drawing Game (ما هذا الرسم؟) ────────────────────────
// ══════════════════════════════════════════════════════════
const drawGames = {};
function getDrawGame(key) {
  if (!drawGames[key]) drawGames[key] = {
    active: false, word: '', hint: '', phase: 'idle',
    winners: [], answerTime: 60, endTimer: null, endTime: 0,
    guessedUsers: new Set(),
  };
  return drawGames[key];
}

app.post('/api/draw/start', (req, res) => {
  const { username, word, hint, answerTime } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !word) return res.json({ ok: false });
  const game = getDrawGame(key);
  if (game.endTimer) clearTimeout(game.endTimer);
  game.active = true;
  game.word = word.trim().toLowerCase().replace(/\s+/g,'');
  game.displayWord = word.trim();
  game.hint = hint || '';
  game.phase = 'drawing';
  game.winners = [];
  game.guessedUsers.clear();
  game.answerTime = parseInt(answerTime) || 60;
  game.endTime = Date.now() + game.answerTime * 1000;
  io.to(`room:${key}`).emit('draw:start', { hint: game.hint, answerTime: game.answerTime, endTime: game.endTime });
  game.endTimer = setTimeout(() => {
    game.active = false;
    game.phase = 'ended';
    io.to(`room:${key}`).emit('draw:end', { word: game.displayWord, winners: game.winners });
    console.log(`[Draw] Time up! Word: "${game.displayWord}" Winners: ${game.winners.length}`);
  }, game.answerTime * 1000);
  console.log(`[Draw] Started: "${word}" hint="${hint}" (${game.answerTime}s)`);
  res.json({ ok: true });
});

app.post('/api/draw/stop', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getDrawGame(key);
  if (game.endTimer) clearTimeout(game.endTimer);
  game.active = false;
  game.phase = 'idle';
  io.to(`room:${key}`).emit('draw:end', { word: game.displayWord || '', winners: game.winners });
  res.json({ ok: true });
});

app.post('/api/draw/clear', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  io.to(`room:${key}`).emit('draw:clear');
  res.json({ ok: true });
});

app.post('/api/draw/hint', (req, res) => {
  const { username, hint } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getDrawGame(key);
  game.hint = hint || '';
  io.to(`room:${key}`).emit('draw:hint', { hint });
  res.json({ ok: true });
});

// ══════════════════════════════════════════════════════════
// ── Letter Race (سباق الحروف) ───────────────────────────
// ══════════════════════════════════════════════════════════
const letterRaceGames = {};
function getLetterRace(key) {
  if (!letterRaceGames[key]) letterRaceGames[key] = {
    active: false, letter: '', wordsNeeded: 3, answerTime: 30,
    phase: 'idle', endTimer: null, endTime: 0,
    players: new Map(), // uid -> { name, avatar, words:[], finished:false, finishTime:0 }
    winners: [], usedWords: new Set(),
  };
  return letterRaceGames[key];
}

const ARABIC_LETTERS = 'ابتثجحخدذرزسشصضطظعغفقكلمنهوي';

app.post('/api/letter-race/start', (req, res) => {
  const { username, letter, wordsNeeded, answerTime } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getLetterRace(key);
  if (game.endTimer) clearTimeout(game.endTimer);
  game.letter = letter || ARABIC_LETTERS[Math.floor(Math.random() * ARABIC_LETTERS.length)];
  game.wordsNeeded = parseInt(wordsNeeded) || 3;
  game.answerTime = parseInt(answerTime) || 30;
  game.active = true;
  game.phase = 'racing';
  game.players.clear();
  game.winners = [];
  game.usedWords.clear();
  game.endTime = Date.now() + game.answerTime * 1000;

  io.to(`room:${key}`).emit('letter-race:start', {
    letter: game.letter, wordsNeeded: game.wordsNeeded,
    answerTime: game.answerTime, endTime: game.endTime,
  });

  game.endTimer = setTimeout(() => {
    game.active = false;
    game.phase = 'ended';
    io.to(`room:${key}`).emit('letter-race:end', { letter: game.letter, winners: game.winners.slice(0, 5) });
    console.log(`[LetterRace] Time up! Letter: "${game.letter}" Winners: ${game.winners.length}`);
  }, game.answerTime * 1000);

  console.log(`[LetterRace] Started: letter="${game.letter}" need=${game.wordsNeeded} time=${game.answerTime}s`);
  res.json({ ok: true, letter: game.letter });
});

app.post('/api/letter-race/stop', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getLetterRace(key);
  if (game.endTimer) clearTimeout(game.endTimer);
  game.active = false;
  game.phase = 'idle';
  io.to(`room:${key}`).emit('letter-race:end', { letter: game.letter, winners: game.winners.slice(0, 5) });
  res.json({ ok: true });
});

// ══════════════════════════════════════════════════════════
// ── Auction (مزاد الكلمات) ──────────────────────────────
// ══════════════════════════════════════════════════════════
const auctionGames = {};
function getAuction(key) {
  if (!auctionGames[key]) auctionGames[key] = {
    active: false, phase: 'idle', word: '', hint: '',
    bids: new Map(), answerTime: 30, endTimer: null, endTime: 0,
    history: [],
  };
  return auctionGames[key];
}

app.post('/api/auction/start', (req, res) => {
  const { username, word, hint, answerTime } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !word) return res.json({ ok: false });
  const game = getAuction(key);
  if (game.endTimer) clearTimeout(game.endTimer);
  game.active = true;
  game.phase = 'bidding';
  game.word = word.trim();
  game.hint = hint || '';
  game.bids.clear();
  game.answerTime = parseInt(answerTime) || 30;
  game.endTime = Date.now() + game.answerTime * 1000;

  io.to(`room:${key}`).emit('auction:start', {
    word: game.word, hint: game.hint,
    answerTime: game.answerTime, endTime: game.endTime,
  });

  game.endTimer = setTimeout(() => {
    game.phase = 'ended';
    game.active = false;
    const sorted = Array.from(game.bids.values()).sort((a,b) => b.likes - a.likes);
    const winner = sorted.length > 0 ? sorted[0] : null;
    if (winner) game.history.push({ word: game.word, winner: winner.name, likes: winner.likes });
    io.to(`room:${key}`).emit('auction:end', {
      word: game.word, winner, top5: sorted.slice(0, 5),
    });
    console.log(`[Auction] Ended: "${game.word}" Winner: ${winner ? winner.name + ' (' + winner.likes + ' likes)' : 'none'}`);
  }, game.answerTime * 1000);

  console.log(`[Auction] Started: "${word}" (${game.answerTime}s)`);
  res.json({ ok: true });
});

app.post('/api/auction/stop', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getAuction(key);
  if (game.endTimer) clearTimeout(game.endTimer);
  const sorted = Array.from(game.bids.values()).sort((a,b) => b.likes - a.likes);
  const winner = sorted.length > 0 ? sorted[0] : null;
  game.active = false; game.phase = 'idle';
  io.to(`room:${key}`).emit('auction:end', { word: game.word, winner, top5: sorted.slice(0, 5) });
  res.json({ ok: true });
});

// ══════════════════════════════════════════════════════════
// ── Ice Game (الجليد) ───────────────────────────────────
// ══════════════════════════════════════════════════════════
const iceGames = {};
function getIceGame(key) {
  if (!iceGames[key]) iceGames[key] = {
    active: false, phase: 'idle', // idle, register, frozen, ended
    players: new Map(), // uid -> {name, avatar, frozen:true, unfreezeTime:0}
    word: '', answerTime: 15, endTimer: null, round: 0,
    keyword: 'جليد', losers: [],
  };
  return iceGames[key];
}

app.post('/api/ice/register', (req, res) => {
  const { username, keyword, maxPlayers } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getIceGame(key);
  if (game.endTimer) clearTimeout(game.endTimer);
  game.players.clear(); game.losers = []; game.round = 0;
  game.phase = 'register'; game.active = true;
  game.keyword = (keyword || 'جليد').toLowerCase().replace(/\s+/g,'');
  game.maxPlayers = parseInt(maxPlayers) || 20;
  io.to(`room:${key}`).emit('ice:register', { keyword: keyword || 'جليد', maxPlayers: game.maxPlayers });
  console.log(`[Ice] Registration opened (max ${game.maxPlayers})`);
  res.json({ ok: true });
});

app.post('/api/ice/lock', (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getIceGame(key);
  game.phase = 'locked';
  io.to(`room:${key}`).emit('ice:locked', { count: game.players.size });
  console.log(`[Ice] Registration locked — ${game.players.size} players`);
  res.json({ ok: true });
});

app.post('/api/ice/freeze', (req, res) => {
  const { username, word, answerTime } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !word) return res.json({ ok: false });
  const game = getIceGame(key);
  if (game.endTimer) clearTimeout(game.endTimer);
  game.round++;
  game.word = word.trim().toLowerCase().replace(/\s+/g,'');
  game.displayWord = word.trim();
  game.answerTime = parseInt(answerTime) || 15;
  game.phase = 'frozen';
  // Freeze all alive players
  for (const [uid, p] of game.players) {
    if (!game.losers.find(l => l.name === p.name)) {
      p.frozen = true;
      p.unfreezeTime = 0;
    }
  }
  const aliveCount = Array.from(game.players.values()).filter(p => !game.losers.find(l => l.name === p.name)).length;
  io.to(`room:${key}`).emit('ice:freeze', {
    word: game.displayWord, answerTime: game.answerTime, round: game.round, aliveCount,
  });
  // Auto end round
  game.endTimer = setTimeout(() => {
    // Last frozen players lose
    const stillFrozen = [];
    for (const [uid, p] of game.players) {
      if (p.frozen && !game.losers.find(l => l.name === p.name)) {
        stillFrozen.push({ name: p.name, avatar: p.avatar });
        game.losers.push({ name: p.name, avatar: p.avatar, round: game.round });
      }
    }
    const alive = Array.from(game.players.values()).filter(p => !game.losers.find(l => l.name === p.name));
    let winner = null;
    if (alive.length <= 1 && alive.length > 0) winner = alive[0];
    game.phase = alive.length <= 1 ? 'ended' : 'frozen';
    io.to(`room:${key}`).emit('ice:round-end', {
      frozen: stillFrozen, losers: game.losers.slice(-5),
      aliveCount: alive.length, round: game.round,
      winner: winner ? { name: winner.name, avatar: winner.avatar } : null,
    });
    console.log(`[Ice] Round ${game.round}: ${stillFrozen.length} frozen out, ${alive.length} alive`);
  }, game.answerTime * 1000);
  console.log(`[Ice] Freeze! Word: "${word}" (${game.answerTime}s) ${aliveCount} alive`);
  res.json({ ok: true });
});

app.post('/api/ice/stop', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getIceGame(key);
  if (game.endTimer) clearTimeout(game.endTimer);
  game.active = false; game.phase = 'idle';
  io.to(`room:${key}`).emit('ice:stopped');
  res.json({ ok: true });
});

app.get('/api/ice/:username', (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const game = getIceGame(key);
  const players = Array.from(game.players.values());
  const alive = players.filter(p => !game.losers.find(l => l.name === p.name));
  res.json({ active: game.active, phase: game.phase, round: game.round, totalPlayers: players.length, aliveCount: alive.length, losers: game.losers, players: players.map(p => ({ name: p.name, avatar: p.avatar })), maxPlayers: game.maxPlayers || 20 });
});

// ══════════════════════════════════════════════════════════
// ── Horse Race (سباق الخيل) ─────────────────────────────
// ══════════════════════════════════════════════════════════
const horseRaceGames = {};
function getHorseRace(key) {
  if (!horseRaceGames[key]) horseRaceGames[key] = {
    active: false, phase: 'idle', // idle, register, racing, finished
    teams: { 1: { name:'أحمر', color:'#ef4444', icon:'🔴', players: new Set(), progress: 0 },
             2: { name:'أزرق', color:'#3b82f6', icon:'🔵', players: new Set(), progress: 0 },
             3: { name:'أخضر', color:'#22c55e', icon:'🟢', players: new Set(), progress: 0 },
             4: { name:'أصفر', color:'#fbbf24', icon:'🟡', players: new Set(), progress: 0 } },
    finishLine: 50, // comments needed to win
    playerTeam: new Map(), // uid -> team number
  };
  return horseRaceGames[key];
}

app.post('/api/horse/start-register', (req, res) => {
  const { username, finishLine } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getHorseRace(key);
  game.finishLine = parseInt(finishLine) || 50;
  game.phase = 'register'; game.active = true;
  game.playerTeam.clear();
  for (let i = 1; i <= 4; i++) { game.teams[i].players.clear(); game.teams[i].progress = 0; }
  io.to(`room:${key}`).emit('horse:register', { finishLine: game.finishLine });
  console.log(`[Horse] Registration opened (finish: ${game.finishLine})`);
  res.json({ ok: true });
});

app.post('/api/horse/lock', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getHorseRace(key);
  game.phase = 'locked';
  const teamCounts = {};
  for (let i = 1; i <= 4; i++) teamCounts[i] = game.teams[i].players.size;
  io.to(`room:${key}`).emit('horse:locked', { teamCounts, total: game.playerTeam.size });
  res.json({ ok: true });
});

app.get('/api/horse/:username', (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const game = getHorseRace(key);
  const players = [];
  for (const [uid, teamNum] of game.playerTeam) {
    const team = game.teams[teamNum];
    players.push({ name: uid, team: teamNum });
  }
  const teamCounts = {};
  for (let i = 1; i <= 4; i++) teamCounts[i] = game.teams[i].players.size;
  res.json({ phase: game.phase, teamCounts, totalPlayers: game.playerTeam.size });
});

app.post('/api/horse/start-race', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getHorseRace(key);
  game.phase = 'racing';
  const teamCounts = {};
  for (let i = 1; i <= 4; i++) teamCounts[i] = game.teams[i].players.size;
  io.to(`room:${key}`).emit('horse:race', { finishLine: game.finishLine, teamCounts });
  console.log(`[Horse] Race started!`);
  res.json({ ok: true });
});

app.post('/api/horse/stop', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getHorseRace(key);
  game.active = false; game.phase = 'idle';
  io.to(`room:${key}`).emit('horse:stopped');
  res.json({ ok: true });
});

// ══════════════════════════════════════════════════════════
// ── Castle War (حرب القلاع) ─────────────────────────────
// ══════════════════════════════════════════════════════════
const castleGames = {};
function getCastleGame(key) {
  if (!castleGames[key]) castleGames[key] = {
    active: false, phase: 'idle', // idle, register, battle, finished
    maxHP: 100,
    red: { hp: 100, players: new Set(), attacks: 0 },
    blue: { hp: 100, players: new Set(), attacks: 0 },
    playerTeam: new Map(), // uid -> 'red'|'blue'
    keyword: 'حرب',
  };
  return castleGames[key];
}

app.post('/api/castle/register', (req, res) => {
  const { username, keyword, maxHP } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getCastleGame(key);
  game.phase = 'register'; game.active = true;
  game.keyword = (keyword || 'حرب').toLowerCase().replace(/\s+/g,'');
  game.maxHP = parseInt(maxHP) || 100;
  game.red = { hp: game.maxHP, players: new Set(), attacks: 0 };
  game.blue = { hp: game.maxHP, players: new Set(), attacks: 0 };
  game.playerTeam.clear();
  io.to(`room:${key}`).emit('castle:register', { keyword: keyword || 'حرب', maxHP: game.maxHP });
  console.log(`[Castle] Registration opened (HP: ${game.maxHP})`);
  res.json({ ok: true });
});

app.post('/api/castle/lock', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getCastleGame(key);
  game.phase = 'locked';
  io.to(`room:${key}`).emit('castle:locked', { redCount: game.red.players.size, blueCount: game.blue.players.size, total: game.playerTeam.size });
  res.json({ ok: true });
});

app.post('/api/castle/battle', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getCastleGame(key);
  game.phase = 'battle';
  io.to(`room:${key}`).emit('castle:battle', {
    maxHP: game.maxHP,
    red: { hp: game.red.hp, players: game.red.players.size },
    blue: { hp: game.blue.hp, players: game.blue.players.size },
  });
  console.log(`[Castle] Battle started! Red: ${game.red.players.size} vs Blue: ${game.blue.players.size}`);
  res.json({ ok: true });
});

app.post('/api/castle/stop', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getCastleGame(key);
  game.active = false; game.phase = 'idle';
  io.to(`room:${key}`).emit('castle:stopped');
  res.json({ ok: true });
});

// ══════════════════════════════════════════════════════════
// ── Russian Roulette (الروليت الروسي) ───────────────────
// ══════════════════════════════════════════════════════════
// ── لعبة الحبار (الضوء الأخضر والأحمر) ─────────────────────
const squidGames = {};
function getSquid(key) {
  if (!squidGames[key]) squidGames[key] = {
    active: false, phase: 'idle', // idle, register, playing, ended
    players: new Map(), // uid -> {name, avatar, steps, alive, finished}
    winners: [], eliminated: [],
    keyword: 'حبار', maxPlayers: 30, finishSteps: 20, winnersCount: 3,
    light: 'red', lightChangedAt: 0,
    auto: false, minGreen: 4, maxGreen: 8, minRed: 3, maxRed: 6, autoTimer: null,
  };
  return squidGames[key];
}
function squidAliveCount(g) {
  let n = 0;
  g.players.forEach(p => { if (p.alive && !p.finished) n++; });
  return n;
}
function squidSetLight(key, color) {
  const g = getSquid(key);
  if (g.light === color) return;
  g.light = color;
  g.lightChangedAt = Date.now();
  broadcast(key, 'squid:light', { light: color });
}
function squidAutoTick(key) {
  const g = getSquid(key);
  if (!g.auto || g.phase !== 'playing') return;
  const next = g.light === 'green' ? 'red' : 'green';
  squidSetLight(key, next);
  const [mn, mx] = next === 'green' ? [g.minGreen, g.maxGreen] : [g.minRed, g.maxRed];
  const dur = (mn + Math.random() * Math.max(0, mx - mn)) * 1000;
  if (g.autoTimer) clearTimeout(g.autoTimer);
  g.autoTimer = setTimeout(() => squidAutoTick(key), dur);
}
function squidEnd(key) {
  const g = getSquid(key);
  g.phase = 'ended';
  g.auto = false;
  if (g.autoTimer) { clearTimeout(g.autoTimer); g.autoTimer = null; }
  broadcast(key, 'squid:end', { winners: g.winners, eliminatedCount: g.eliminated.length });
}

app.post('/api/squid/register', (req, res) => {
  const { username, keyword, maxPlayers, finishSteps, winnersCount } = req.body;
  const key = username?.toLowerCase().replace('@', '').trim();
  if (!key) return res.json({ ok: false });
  const g = getSquid(key);
  if (g.autoTimer) { clearTimeout(g.autoTimer); g.autoTimer = null; }
  g.active = true; g.phase = 'register'; g.auto = false;
  g.players = new Map(); g.winners = []; g.eliminated = [];
  g.keyword = (keyword || 'حبار').trim();
  g.maxPlayers = Math.max(2, Math.min(100, parseInt(maxPlayers) || 30));
  g.finishSteps = Math.max(5, Math.min(100, parseInt(finishSteps) || 20));
  g.winnersCount = Math.max(1, Math.min(10, parseInt(winnersCount) || 3));
  g.light = 'red'; g.lightChangedAt = Date.now();
  broadcast(key, 'squid:register', { keyword: g.keyword, maxPlayers: g.maxPlayers, finishSteps: g.finishSteps });
  res.json({ ok: true });
});

app.post('/api/squid/start', (req, res) => {
  const { username, auto, minGreen, maxGreen, minRed, maxRed } = req.body;
  const key = username?.toLowerCase().replace('@', '').trim();
  if (!key) return res.json({ ok: false });
  const g = getSquid(key);
  if (!g.active || !g.players.size) return res.json({ ok: false, message: 'لا يوجد لاعبون' });
  g.phase = 'playing';
  g.minGreen = Math.max(1, parseFloat(minGreen) || 4);
  g.maxGreen = Math.max(g.minGreen, parseFloat(maxGreen) || 8);
  g.minRed = Math.max(1, parseFloat(minRed) || 3);
  g.maxRed = Math.max(g.minRed, parseFloat(maxRed) || 6);
  broadcast(key, 'squid:start', {
    players: Array.from(g.players.values()).map(p => ({ name: p.name, avatar: p.avatar, steps: p.steps })),
    finishSteps: g.finishSteps, count: g.players.size,
  });
  squidSetLight(key, 'green');
  if (auto) { g.auto = true; const dur = (g.minGreen + Math.random() * (g.maxGreen - g.minGreen)) * 1000; g.autoTimer = setTimeout(() => squidAutoTick(key), dur); }
  res.json({ ok: true });
});

app.post('/api/squid/light', (req, res) => {
  const { username, color } = req.body;
  const key = username?.toLowerCase().replace('@', '').trim();
  if (!key || !['green', 'red'].includes(color)) return res.json({ ok: false });
  const g = getSquid(key);
  g.auto = false;
  if (g.autoTimer) { clearTimeout(g.autoTimer); g.autoTimer = null; }
  squidSetLight(key, color);
  res.json({ ok: true, light: color });
});

app.post('/api/squid/auto', (req, res) => {
  const { username, on } = req.body;
  const key = username?.toLowerCase().replace('@', '').trim();
  if (!key) return res.json({ ok: false });
  const g = getSquid(key);
  g.auto = !!on;
  if (g.autoTimer) { clearTimeout(g.autoTimer); g.autoTimer = null; }
  if (g.auto && g.phase === 'playing') squidAutoTick(key);
  res.json({ ok: true, auto: g.auto });
});

app.post('/api/squid/stop', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@', '').trim();
  if (!key) return res.json({ ok: false });
  const g = getSquid(key);
  g.active = false; g.phase = 'idle'; g.auto = false;
  if (g.autoTimer) { clearTimeout(g.autoTimer); g.autoTimer = null; }
  broadcast(key, 'squid:stopped', {});
  res.json({ ok: true });
});

app.get('/api/squid/:username', (req, res) => {
  const key = req.params.username?.toLowerCase().replace('@', '').trim();
  const g = getSquid(key);
  res.json({
    active: g.active, phase: g.phase, light: g.light, auto: g.auto,
    keyword: g.keyword, maxPlayers: g.maxPlayers, finishSteps: g.finishSteps, winnersCount: g.winnersCount,
    players: Array.from(g.players.values()).map(p => ({ name: p.name, avatar: p.avatar, steps: p.steps, alive: p.alive, finished: p.finished })),
    winners: g.winners, eliminated: g.eliminated,
  });
});

const rouletteGames = {};
function getRoulette(key) {
  if (!rouletteGames[key]) rouletteGames[key] = {
    active: false, phase: 'idle', // idle, register, spinning, ended
    players: new Map(), // uid -> {name, avatar, alive}
    eliminated: [], round: 0, keyword: 'روليت',
    maxPlayers: 6,
  };
  return rouletteGames[key];
}

app.post('/api/roulette/register', (req, res) => {
  const { username, keyword, maxPlayers } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getRoulette(key);
  game.players.clear(); game.eliminated = []; game.round = 0;
  game.phase = 'register'; game.active = true;
  game.keyword = (keyword || 'روليت').toLowerCase().replace(/\s+/g,'');
  game.maxPlayers = parseInt(maxPlayers) || 6;
  io.to(`room:${key}`).emit('roulette:register', { keyword: keyword || 'روليت', maxPlayers: game.maxPlayers });
  res.json({ ok: true });
});

app.post('/api/roulette/lock', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getRoulette(key);
  game.phase = 'locked';
  io.to(`room:${key}`).emit('roulette:locked', { count: game.players.size });
  res.json({ ok: true });
});

app.post('/api/roulette/spin', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getRoulette(key);
  if (!game.active) return res.json({ ok: false });
  game.round++;
  game.phase = 'spinning';
  // Pick random alive player
  const alive = Array.from(game.players.entries()).filter(([uid, p]) => p.alive);
  if (alive.length <= 1) return res.json({ ok: false, msg: 'not enough players' });
  const victimIdx = Math.floor(Math.random() * alive.length);
  const [victimUid, victim] = alive[victimIdx];
  victim.alive = false;
  game.eliminated.push({ name: victim.name, avatar: victim.avatar, round: game.round });
  const remaining = Array.from(game.players.values()).filter(p => p.alive);
  let winner = null;
  if (remaining.length <= 1) {
    winner = remaining.length === 1 ? remaining[0] : null;
    game.phase = 'ended';
    game.active = false;
  }
  io.to(`room:${key}`).emit('roulette:result', {
    victim: { name: victim.name, avatar: victim.avatar },
    round: game.round,
    aliveCount: remaining.length,
    alive: remaining.map(p => ({ name: p.name, avatar: p.avatar })),
    eliminated: game.eliminated.slice(-6),
    winner: winner ? { name: winner.name, avatar: winner.avatar } : null,
  });
  console.log(`[Roulette] Round ${game.round}: ${victim.name} eliminated! ${remaining.length} alive`);
  res.json({ ok: true, victim: victim.name, aliveCount: remaining.length });
});

app.post('/api/roulette/stop', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getRoulette(key);
  game.active = false; game.phase = 'idle';
  io.to(`room:${key}`).emit('roulette:stopped');
  res.json({ ok: true });
});

app.get('/api/roulette/:username', (req, res) => {
  const key = req.params.username?.toLowerCase().replace('@','').trim();
  const game = getRoulette(key);
  res.json({
    active: game.active, phase: game.phase, round: game.round,
    keyword: game.keyword, maxPlayers: game.maxPlayers,
    players: Array.from(game.players.values()).map(p => ({ name: p.name, avatar: p.avatar, alive: p.alive })),
    eliminated: game.eliminated,
  });
});

// ══════════════════════════════════════════════════════════
// ── Guess Time (خمن الوقت) — 4 phases ──────────────────
// ══════════════════════════════════════════════════════════
const guessTimeGames = {};
function getGuessTime(key) {
  if (!guessTimeGames[key]) guessTimeGames[key] = {
    state: 'idle', // idle, visible, hidden, guessing, ended
    startedAt: 0, stoppedAt: 0,
    visibleDuration: 5, guessDuration: 20,
    guesses: new Map(), // uid -> {userId, name, avatar, guess, diff}
    eligible: new Set(), // uids eligible to guess (sent gift)
    hideTimer: null, guessTimer: null,
  };
  return guessTimeGames[key];
}

app.post('/api/guess-time/start', (req, res) => {
  const { username, visibleDuration, guessDuration } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getGuessTime(key);
  if (game.hideTimer) clearTimeout(game.hideTimer);
  if (game.guessTimer) clearTimeout(game.guessTimer);
  game.state = 'visible';
  game.startedAt = Date.now();
  game.stoppedAt = 0;
  game.visibleDuration = parseInt(visibleDuration) || 5;
  game.guessDuration = parseInt(guessDuration) || 20;
  game.guesses.clear();
  game.eligible.clear();
  io.to(`room:${key}`).emit('gtime:started', {});
  // Auto-hide after visibleDuration (if > 0)
  if (game.visibleDuration > 0) {
    game.hideTimer = setTimeout(() => {
      if (game.state === 'visible') {
        game.state = 'hidden';
        io.to(`room:${key}`).emit('gtime:hidden', {});
        console.log(`[GuessTime] Auto-hidden @${key}`);
      }
    }, game.visibleDuration * 1000);
  }
  console.log(`[GuessTime] Started @${key} (visible=${game.visibleDuration}s, guess=${game.guessDuration}s)`);
  res.json({ ok: true });
});

app.post('/api/guess-time/hide', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getGuessTime(key);
  if (game.state !== 'visible') return res.json({ ok: false });
  if (game.hideTimer) clearTimeout(game.hideTimer);
  game.state = 'hidden';
  io.to(`room:${key}`).emit('gtime:hidden', {});
  res.json({ ok: true });
});

app.post('/api/guess-time/stop', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getGuessTime(key);
  if (!['visible','hidden'].includes(game.state)) return res.json({ ok: false });
  if (game.hideTimer) clearTimeout(game.hideTimer);
  game.stoppedAt = Date.now() - game.startedAt;
  game.state = 'guessing';
  io.to(`room:${key}`).emit('gtime:stopped', { stoppedAt: game.stoppedAt, guessDuration: game.guessDuration });
  // Auto-end guessing after guessDuration
  game.guessTimer = setTimeout(() => {
    if (game.state === 'guessing') endGuessTime(key);
  }, game.guessDuration * 1000);
  console.log(`[GuessTime] Stopped @${key} at ${(game.stoppedAt/1000).toFixed(2)}s — guessing open for ${game.guessDuration}s`);
  res.json({ ok: true, stoppedAt: game.stoppedAt });
});

app.post('/api/guess-time/end', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  endGuessTime(key);
  res.json({ ok: true });
});

app.post('/api/guess-time/reset', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  const game = getGuessTime(key);
  if (game.hideTimer) clearTimeout(game.hideTimer);
  if (game.guessTimer) clearTimeout(game.guessTimer);
  game.state = 'idle';
  game.guesses.clear();
  game.eligible.clear();
  io.to(`room:${key}`).emit('gtime:reset', {});
  res.json({ ok: true });
});

app.get('/api/guess-time/:username', (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const game = getGuessTime(key);
  res.json({ state: game.state, stoppedAt: game.stoppedAt, eligibleCount: game.eligible.size, guessCount: game.guesses.size, guessDuration: game.guessDuration });
});

function endGuessTime(key) {
  const game = getGuessTime(key);
  if (game.guessTimer) clearTimeout(game.guessTimer);
  game.state = 'ended';
  const stoppedSec = game.stoppedAt / 1000;
  const results = Array.from(game.guesses.values()).map(g => ({ ...g, diff: Math.abs(g.guess - stoppedSec) })).sort((a,b) => a.diff - b.diff);
  const winner = results.length > 0 ? results[0] : null;
  io.to(`room:${key}`).emit('gtime:ended', { stoppedAt: game.stoppedAt, winner, allGuesses: results });
  console.log(`[GuessTime] Ended @${key}: winner=${winner?.name || 'none'} (${winner?.guess}s, diff ${winner?.diff?.toFixed(2)}s)`);
}

// ⛔ heartbeat إعادة الاتصال حُذف نهائياً — كان يعيد الاتصال كل دقيقتين بلا توقف

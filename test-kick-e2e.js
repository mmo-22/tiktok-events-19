/**
 * اختبار شامل: سيرفر Kick وهمي + السيرفر الحقيقي + محاكاة حرب الكلمات والخروج
 */
const express = require('express');
const { io: ioClient } = require('socket.io-client');
const cp = require('child_process');

const FAKE_PORT = 4101;
const APP_PORT = 4102;
const ROOM = 'kick:testchannel';
const BID = 555000;

// ── 1) سيرفر Kick وهمي ───────────────────────────────────
const fake = express();
fake.use(express.urlencoded({ extended: true }));
fake.use(express.json());
fake.post('/oauth/token', (_q, r) => r.json({ access_token: 'FAKE', expires_in: 3600 }));
fake.get('/channels', (_q, r) => r.json({ data: [{ broadcaster_user_id: BID, slug: 'testchannel' }] }));
fake.post('/events/subscriptions', (_q, r) => r.json({ data: [{ subscription_id: 'sub1' }] }));
fake.delete('/events/subscriptions', (_q, r) => r.json({ data: [] }));
fake.get('/livestreams', (_q, r) => r.json({ data: [{ viewer_count: 1234 }] }));

async function main() {
  await new Promise((res) => fake.listen(FAKE_PORT, res));
  console.log('✅ سيرفر Kick الوهمي شغّال');

  // ── 2) السيرفر الحقيقي ────────────────────────────────
  const env = {
    ...process.env,
    PORT: String(APP_PORT),
    EULER_API_KEY: 'x',
    KICK_CLIENT_ID: 'id',
    KICK_CLIENT_SECRET: 'secret',
    KICK_VERIFY_SIGNATURE: 'false',
    KICK_API_BASE: `http://localhost:${FAKE_PORT}`,
    KICK_ID_BASE: `http://localhost:${FAKE_PORT}`,
    KICK_VIEWER_POLL_MS: '2000',
  };
  const srv = cp.spawn('node', ['src/server.js'], { env, cwd: __dirname });
  let log = '';
  srv.stdout.on('data', (d) => { log += d; });
  srv.stderr.on('data', (d) => { log += d; });
  await sleep(2500);

  const base = `http://localhost:${APP_PORT}`;
  const post = (p, b) => fetch(base + p, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(b),
  }).then((r) => r.json().catch(() => ({})));

  const hook = (type, payload) => fetch(`${base}/webhooks/kick`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Kick-Event-Type': type },
    body: JSON.stringify({ broadcaster: { user_id: BID }, ...payload }),
  });

  const chat = (user, text) => hook('chat.message.sent', {
    message_id: `m${Math.random()}`,
    content: text,
    sender: { user_id: 9000 + user, username: `user${user}`, channel_slug: `user${user}`, profile_picture: null },
  });

  // ── 3) عميل socket يراقب الأحداث ─────────────────────
  const events = [];
  const sock = ioClient(base, { transports: ['websocket'] });
  await new Promise((res) => sock.on('connect', res));
  sock.emit('join', { username: ROOM });
  for (const e of ['chat', 'word-war:join', 'word-war:word', 'knockout:joined', 'knockout:answered', 'stats', 'viewers', 'gift', 'follow']) {
    sock.on(e, (d) => events.push({ e, d }));
  }
  await sleep(300);

  // ── 4) اتصال غرفة كيك ────────────────────────────────
  sock.emit('connect-room', { username: ROOM });
  await sleep(2500);

  const results = [];
  const check = (name, cond, extra = '') => {
    results.push({ name, ok: !!cond, extra });
    console.log(`${cond ? '✅' : '❌'} ${name} ${extra}`);
  };

  check('الاتصال بغرفة كيك', log.includes('Kick] Subscribing') || log.includes('Connected'));

  // ── 5) تعليق عادي ────────────────────────────────────
  await chat(1, 'مرحبا');
  await sleep(500);
  const chatEv = events.find((x) => x.e === 'chat');
  check('وصول التعليقات', chatEv && chatEv.d.comment === 'مرحبا', chatEv ? `(${chatEv.d.user})` : '');
  check('اسم المستخدم صحيح', chatEv && chatEv.d.user === 'user1', chatEv ? `→ ${chatEv.d.user}` : '');

  // ── 6) حرب الكلمات ───────────────────────────────────
  await post('/api/word-war/open-registration', { username: ROOM, redKeyword: 'احمر', blueKeyword: 'ازرق' });
  await sleep(200);
  await chat(2, 'احمر');
  await chat(3, 'ازرق');
  await chat(4, 'احمر');
  await sleep(600);
  const joins = events.filter((x) => x.e === 'word-war:join');
  check('حرب الكلمات: انضمام الفرق', joins.length === 3, `(${joins.length}/3)`);
  const last = joins[joins.length - 1];
  check('حرب الكلمات: عدّ اللاعبين', last && last.d.redCount === 2 && last.d.blueCount === 1,
    last ? `أحمر=${last.d.redCount} أزرق=${last.d.blueCount}` : '');

  await post('/api/word-war/start', { username: ROOM, category: 'فواكه', duration: 60 });
  await sleep(300);
  await chat(2, 'تفاح');
  await chat(3, 'موز');
  await chat(4, 'برتقال');
  await chat(2, 'تفاح'); // مكررة — يفترض تُتجاهل
  await sleep(700);
  const words = events.filter((x) => x.e === 'word-war:word');
  check('حرب الكلمات: احتساب الكلمات', words.length === 3, `(${words.length}/3 — المكررة اتجاهلت)`);
  const lastW = words[words.length - 1];
  check('حرب الكلمات: النتيجة', lastW && lastW.d.redScore === 2 && lastW.d.blueScore === 1,
    lastW ? `أحمر=${lastW.d.redScore} أزرق=${lastW.d.blueScore}` : '');
  await post('/api/word-war/stop', { username: ROOM });

  // ── 7) فعالية الخروج (Knockout) ──────────────────────
  await post('/api/knockout/register', { username: ROOM, keyword: 'بطولة', maxPlayers: 10 });
  await sleep(200);
  await chat(5, 'بطولة');
  await chat(6, 'بطولة');
  await chat(7, 'بطولة');
  await sleep(700);
  const koJoin = events.filter((x) => x.e === 'knockout:joined');
  check('الخروج: تسجيل اللاعبين', koJoin.length === 3, `(${koJoin.length}/3)`);
  const lastK = koJoin[koJoin.length - 1];
  check('الخروج: العدّاد', lastK && lastK.d.count === 3, lastK ? `count=${lastK.d.count}` : '');

  await post('/api/knockout/lock', { username: ROOM });
  await post('/api/knockout/ask', {
    username: ROOM, question: 'كم يساوي ٢+٢؟', options: ['٣', '٤', '٥'], correct: 1, duration: 30,
  });
  await sleep(300);
  await chat(5, '2');
  await chat(6, '2');
  await sleep(700);
  const koAns = events.filter((x) => x.e === 'knockout:answered');
  check('الخروج: استقبال الإجابات', koAns.length >= 1, `(${koAns.length} حدث)`);
  await post('/api/knockout/stop', { username: ROOM });

  // ── 8) الهدايا (Kicks) ───────────────────────────────
  await hook('kicks.gifted', {
    sender: { user_id: 7777, username: 'donor', channel_slug: 'donor' },
    gift: { name: 'Rocket', gift_id: 'r1', amount: 500 },
  });
  await sleep(500);
  const giftEv = events.find((x) => x.e === 'gift');
  check('الهدايا (Kicks)', giftEv && giftEv.d.diamondCount === 500,
    giftEv ? `${giftEv.d.giftName}=${giftEv.d.diamondCount}` : '');

  // ── 9) المتابعة ──────────────────────────────────────
  await hook('channel.followed', { follower: { user_id: 8888, username: 'newfan', channel_slug: 'newfan' } });
  await sleep(400);
  const folEv = events.find((x) => x.e === 'follow');
  check('المتابعات', folEv && folEv.d.user === 'newfan', folEv ? `→ ${folEv.d.user}` : '');

  // ── 10) عدد المشاهدين ────────────────────────────────
  await sleep(2500);
  const viewEv = events.filter((x) => x.e === 'viewers').pop();
  check('عدد المشاهدين (استعلام دوري)', viewEv && viewEv.d.count === 1234,
    viewEv ? `${viewEv.d.count}` : 'لم يصل');

  // ── النتيجة ──────────────────────────────────────────
  const passed = results.filter((r) => r.ok).length;
  console.log(`\n═══ ${passed}/${results.length} نجحت ═══`);
  const failed = results.filter((r) => !r.ok);
  if (failed.length) {
    console.log('\nسجل السيرفر:\n' + log.slice(-2500));
  }
  sock.close();
  srv.kill();
  process.exit(failed.length ? 1 : 0);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
main().catch((e) => { console.error('فشل الاختبار:', e); process.exit(1); });

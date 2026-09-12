'use strict';
/**
 * مزوّد Kick — واجهة شبيهة بـ WebSocket فوق Kick Public API
 * ---------------------------------------------------------
 * الفكرة: server.js يتوقع كائن ws يطلق 'open' / 'message' / 'close' / 'error'،
 * والرسالة تكون JSON بصيغة EulerStream: { messages: [ { event, data } ] }
 * فنترجم أحداث Kick لنفس الصيغة، وبقية المشروع تشتغل بدون أي تعديل.
 *
 * Kick الرسمي يعتمد Webhooks (HTTP POST) مو WebSocket، فنحتاج:
 *   1) توكن تطبيق (client_credentials)
 *   2) تحويل slug القناة → broadcaster_user_id
 *   3) اشتراك بالأحداث عبر POST /events/subscriptions
 *   4) استقبال الـ POST على /webhooks/kick وتوجيهه للغرفة الصحيحة
 *   5) استعلام دوري لعدد المشاهدين (ما فيه حدث لحظي له)
 *
 * مرجع: https://docs.kick.com
 */

const EventEmitter = require('events');

const KICK_API = process.env.KICK_API_BASE || 'https://api.kick.com/public/v1';
const KICK_ID = process.env.KICK_ID_BASE || 'https://id.kick.com';
const CLIENT_ID = process.env.KICK_CLIENT_ID;
const CLIENT_SECRET = process.env.KICK_CLIENT_SECRET;
const VIEWER_POLL_MS = Number(process.env.KICK_VIEWER_POLL_MS || 30000);

// الأحداث اللي نشترك فيها (الاسم + النسخة)
const SUBSCRIBED_EVENTS = [
  { name: 'chat.message.sent', version: 1 },
  { name: 'channel.followed', version: 1 },
  { name: 'channel.subscription.new', version: 1 },
  { name: 'channel.subscription.renewal', version: 1 },
  { name: 'channel.subscription.gifts', version: 1 },
  { name: 'kicks.gifted', version: 1 },
  { name: 'livestream.status.updated', version: 1 },
];

// ── سجل الغرف النشطة: broadcaster_user_id → connection ────
const registry = new Map();

// ── توكن التطبيق (مشترك بين كل الغرف) ─────────────────────
let tokenCache = { value: null, expiresAt: 0 };

async function getAppToken() {
  if (tokenCache.value && Date.now() < tokenCache.expiresAt - 60000) return tokenCache.value;
  if (!CLIENT_ID || !CLIENT_SECRET) throw new Error('KICK_CLIENT_ID / KICK_CLIENT_SECRET غير مضبوطة');

  const body = new URLSearchParams({
    grant_type: 'client_credentials',
    client_id: CLIENT_ID,
    client_secret: CLIENT_SECRET,
  });
  const res = await fetch(`${KICK_ID}/oauth/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body,
  });
  if (!res.ok) throw new Error(`فشل الحصول على توكن Kick (${res.status})`);
  const data = await res.json();
  tokenCache = {
    value: data.access_token,
    expiresAt: Date.now() + (data.expires_in || 3600) * 1000,
  };
  return tokenCache.value;
}

async function kickFetch(path, opts = {}) {
  const token = await getAppToken();
  const res = await fetch(`${KICK_API}${path}`, {
    ...opts,
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
      ...(opts.headers || {}),
    },
  });
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch (_) { /* رد غير JSON */ }
  if (!res.ok) {
    const err = new Error(`Kick ${path} → ${res.status}: ${text.slice(0, 200)}`);
    err.status = res.status;
    throw err;
  }
  return json;
}

// ── تحويل مستخدم Kick إلى شكل EulerStream ─────────────────
function mapUser(k = {}) {
  const badges = k.identity?.badges || [];
  const has = (t) => badges.some((b) => b.type === t);
  return {
    nickname: k.username || k.channel_slug || 'unknown',
    uniqueId: k.channel_slug || k.username || '',
    userId: String(k.user_id || k.channel_slug || ''),
    profilePicture: { urls: k.profile_picture ? [k.profile_picture] : [] },
    isModerator: has('moderator') || has('broadcaster'),
    isSubscriber: has('subscriber') || has('founder'),
    followRole: 0,
  };
}

/**
 * ترجمة حدث Kick → أحداث داخلية بصيغة المشروع.
 * ترجع مصفوفة (ممكن حدث واحد يولّد أكثر من حدث داخلي).
 */
function translate(eventType, payload) {
  switch (eventType) {
    case 'chat.message.sent':
      return [{
        event: 'chat',
        data: {
          msgId: payload.message_id,
          comment: payload.content || '',
          user: mapUser(payload.sender),
        },
      }];

    case 'channel.followed':
      return [{ event: 'follow', data: { user: mapUser(payload.follower || payload.user) } }];

    // الاشتراكات والـ Kicks تُعامل كهدايا حتى تشتغل ألعاب الهدايا
    case 'channel.subscription.new':
    case 'channel.subscription.renewal':
      return [{
        event: 'gift',
        data: {
          giftType: 0,
          giftName: 'Subscription',
          giftId: 'kick_sub',
          repeatCount: payload.duration || 1,
          diamondCount: (payload.duration || 1) * 100, // وزن تقديري — عدّله كيف ما تبي
          user: mapUser(payload.subscriber || payload.user),
        },
      }];

    case 'channel.subscription.gifts': {
      const giftees = payload.giftees || [];
      return [{
        event: 'gift',
        data: {
          giftType: 0,
          giftName: 'Gifted Subs',
          giftId: 'kick_gift_sub',
          repeatCount: giftees.length || 1,
          diamondCount: (giftees.length || 1) * 100,
          user: mapUser(payload.gifter),
        },
      }];
    }

    case 'kicks.gifted':
      return [{
        event: 'gift',
        data: {
          giftType: 0,
          giftName: payload.gift?.name || 'Kicks',
          giftId: payload.gift?.gift_id || 'kicks',
          giftPictureUrl: payload.gift?.gift_url || null,
          repeatCount: 1,
          diamondCount: payload.gift?.amount || payload.amount || 0,
          user: mapUser(payload.sender),
        },
      }];

    case 'livestream.status.updated':
      if (payload.is_live === false) return [{ event: 'streamEnd', data: {} }];
      return [];

    default:
      return [];
  }
}

// ── الاتصال (واجهة شبيهة بـ WebSocket) ────────────────────
class KickConnection extends EventEmitter {
  constructor(slug) {
    super();
    this.slug = slug;
    this.readyState = 0; // CONNECTING
    this.broadcasterUserId = null;
    this.subscriptionIds = [];
    this.viewerTimer = null;
    setImmediate(() => this._start());
  }

  async _start() {
    try {
      const ch = await kickFetch(`/channels?slug=${encodeURIComponent(this.slug)}`);
      const channel = ch?.data?.[0];
      if (!channel) throw new Error(`القناة @${this.slug} غير موجودة`);
      this.broadcasterUserId = channel.broadcaster_user_id;

      const sub = await kickFetch('/events/subscriptions', {
        method: 'POST',
        body: JSON.stringify({
          broadcaster_user_id: this.broadcasterUserId,
          events: SUBSCRIBED_EVENTS,
          method: 'webhook',
        }),
      });
      this.subscriptionIds = (sub?.data || [])
        .map((s) => s.subscription_id)
        .filter(Boolean);

      registry.set(String(this.broadcasterUserId), this);
      this.readyState = 1; // OPEN
      this.emit('open');

      this._pollViewers();
      this.viewerTimer = setInterval(() => this._pollViewers(), VIEWER_POLL_MS);
    } catch (err) {
      this.readyState = 3;
      this.emit('error', err);
      // 401/403 = مشكلة مفتاح أو صلاحية → نفس معنى INVALID_AUTH عند EulerStream
      const code = err.status === 401 || err.status === 403 ? 4401 : 1011;
      this.emit('close', code, err.message);
    }
  }

  async _pollViewers() {
    if (this.readyState !== 1) return;
    try {
      const res = await kickFetch(`/livestreams?broadcaster_user_id=${this.broadcasterUserId}`);
      const stream = res?.data?.[0];
      if (!stream) return; // غير مباشر حالياً
      this._deliver([{ event: 'roomUserSeq', data: { viewerCount: stream.viewer_count || 0 } }]);
    } catch (_) { /* تجاهل فشل استعلام واحد */ }
  }

  /** يحقن أحداث داخلية كأنها رسالة WebSocket */
  _deliver(messages) {
    if (!messages.length || this.readyState !== 1) return;
    this.emit('message', Buffer.from(JSON.stringify({ messages })));
  }

  /** يُستدعى من راوت الـ webhook */
  handleWebhook(eventType, payload) {
    this._deliver(translate(eventType, payload));
  }

  ping() { /* لا يوجد ping — الاتصال HTTP */ }

  async close() {
    if (this.readyState === 3) return;
    this.readyState = 3;
    if (this.viewerTimer) { clearInterval(this.viewerTimer); this.viewerTimer = null; }
    if (this.broadcasterUserId) registry.delete(String(this.broadcasterUserId));
    for (const id of this.subscriptionIds) {
      try { await kickFetch(`/events/subscriptions?id=${id}`, { method: 'DELETE' }); } catch (_) {}
    }
    this.subscriptionIds = [];
    this.emit('close', 1000, 'closed by server');
  }

  terminate() { return this.close(); }
}

function connect(slug) {
  return new KickConnection(slug);
}

/** توجيه حدث webhook وارد للغرفة الصحيحة */
function routeWebhook(eventType, payload) {
  const id = String(
    payload?.broadcaster?.user_id ??
    payload?.broadcaster_user_id ??
    ''
  );
  const conn = id && registry.get(id);
  if (!conn) return false;
  conn.handleWebhook(eventType, payload);
  return true;
}

module.exports = { connect, routeWebhook, registry, getAppToken, kickFetch, translate, mapUser };

'use strict';
/**
 * تحقق من توقيع webhook القادم من Kick.
 *
 * Kick يوقّع كل طلب بمفتاح RSA خاص، ويرسل:
 *   Kick-Event-Message-Id
 *   Kick-Event-Message-Timestamp
 *   Kick-Event-Signature   (base64)
 * والنص الموقَّع هو:  `${messageId}.${timestamp}.${rawBody}`
 * والمفتاح العام يُجلب من:  GET /public-key
 *
 * ملاحظة: راجع https://docs.kick.com/events/webhook-security قبل الإنتاج —
 * لو تغيّرت الصيغة عدّل SIGNED_TEMPLATE أدناه فقط.
 *
 * للتعطيل أثناء التطوير: KICK_VERIFY_SIGNATURE=false
 */

const crypto = require('crypto');

const VERIFY = process.env.KICK_VERIFY_SIGNATURE !== 'false';
const TOLERANCE_MS = Number(process.env.KICK_TIMESTAMP_TOLERANCE_MS || 5 * 60 * 1000);
const KICK_API = process.env.KICK_API_BASE || 'https://api.kick.com/public/v1';

let publicKeyCache = null;
const seenMessageIds = new Map(); // منع إعادة التشغيل (replay)

async function getPublicKey() {
  if (publicKeyCache) return publicKeyCache;
  const res = await fetch(`${KICK_API}/public-key`);
  if (!res.ok) throw new Error(`فشل جلب مفتاح Kick العام (${res.status})`);
  const json = await res.json();
  publicKeyCache = json?.data?.public_key || json?.public_key;
  if (!publicKeyCache) throw new Error('رد /public-key ما فيه مفتاح');
  return publicKeyCache;
}

function rememberMessageId(id) {
  const now = Date.now();
  for (const [k, t] of seenMessageIds) {
    if (now - t > TOLERANCE_MS * 2) seenMessageIds.delete(k);
  }
  if (seenMessageIds.has(id)) return false; // مكرر
  seenMessageIds.set(id, now);
  return true;
}

/**
 * @param {object} headers  رؤوس الطلب (lowercase كما يعطيها express)
 * @param {Buffer} rawBody  جسم الطلب الخام — لازم يكون الخام مو معاد تحويله
 * @returns {Promise<{ok: boolean, reason?: string}>}
 */
async function verify(headers, rawBody) {
  if (!VERIFY) return { ok: true, reason: 'verification disabled' };

  const messageId = headers['kick-event-message-id'];
  const timestamp = headers['kick-event-message-timestamp'];
  const signature = headers['kick-event-signature'];
  if (!messageId || !timestamp || !signature) return { ok: false, reason: 'رؤوس التوقيع ناقصة' };

  const ts = Date.parse(timestamp) || Number(timestamp);
  if (!ts || Math.abs(Date.now() - ts) > TOLERANCE_MS) {
    return { ok: false, reason: 'طابع زمني خارج النطاق المسموح' };
  }
  if (!rememberMessageId(messageId)) return { ok: false, reason: 'رسالة مكررة' };

  try {
    const publicKey = await getPublicKey();
    const signed = Buffer.concat([
      Buffer.from(`${messageId}.${timestamp}.`, 'utf8'),
      rawBody,
    ]);
    const ok = crypto.verify(
      'sha256',
      signed,
      { key: publicKey, padding: crypto.constants.RSA_PKCS1_PADDING },
      Buffer.from(signature, 'base64')
    );
    return ok ? { ok: true } : { ok: false, reason: 'توقيع غير مطابق' };
  } catch (err) {
    return { ok: false, reason: err.message };
  }
}

module.exports = { verify };

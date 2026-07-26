# 🎯 TikTok Live Reader — نسخة EulerStream

قارئ chat ومنصة فعاليات للبث المباشر على TikTok، يعتمد على سيرفر **[EulerStream](https://www.eulerstream.com/)**.

## المتطلبات
- Node.js 18+
- حساب ومفتاح API من [EulerStream Dashboard](https://www.eulerstream.com/dashboard)

## الإعداد

```bash
npm install
cp .env.example .env      # ثم ضع مفتاحك داخل الملف
npm start
```

ثم افتح: **http://localhost:3000**

### متغيرات البيئة

| المتغير | إلزامي | الوصف |
|---|---|---|
| `EULER_API_KEY` | ✅ | مفتاح EulerStream |
| `EULER_WS_URL` | ❌ | عنوان سيرفر الـ WS (افتراضي `wss://ws.eulerstream.com`) |
| `PORT` | ❌ | منفذ السيرفر (افتراضي 3000) |

> على Railway: أضف `EULER_API_KEY` في **Variables**.
> المتغير القديم `TIKTOOL_API_KEY` لا يزال مقروءاً كاحتياط، لكن يُفضّل التحويل للاسم الجديد.

## كيف يشتغل الاتصال

السيرفر يفتح WebSocket واحد لكل غرفة:

```
wss://ws.eulerstream.com?uniqueId=USERNAME&apiKey=KEY
  &features.bundleEvents=true
  &features.normalizeUniqueId=true
  &features.syntheticPresence=true
```

- المفتاح يبقى في السيرفر فقط ولا يصل للمتصفح.
- `bundleEvents` يجعل الأحداث تصل مجمّعة داخل `packet.messages` — نفس الصيغة المستخدمة في الكود.
- المتصفح يتواصل مع السيرفر عبر Socket.IO فقط.

### أكواد الإغلاق (EulerStream ClientCloseCode)

| الكود | المعنى | السلوك |
|---|---|---|
| 4400 / 4401 / 4403 | إعدادات أو مفتاح أو صلاحية خاطئة | إيقاف فوري، يحتاج تدخل يدوي |
| 4404 | البث غير مفتوح | إعادة محاولة منضبطة |
| 4005 | انتهى البث | إعادة محاولة منضبطة |
| 4429 | تجاوز عدد الاتصالات المسموح | محاولة واحدة بعد 10 دقائق ثم إيقاف |
| 4500 / 4556 / 4557 | خطأ من جهة TikTok | إعادة محاولة منضبطة |

## الميزات
- 💬 Chat مباشر
- 🎁 الهدايا والألماس
- 👁️ عدد المشاهدين
- ❤️ الإعجابات والمتابعات والمشاركات
- 🎮 ألعاب وأوفرلايات جاهزة (عجلة، مزاد، حرب كلمات، لعبة الحبار، سباق خيل، وغيرها)
- 🛠️ لوحة تحكم `admin.html` + صفحات OBS

## ملاحظة
EulerStream خدمة مدفوعة — راجع [صفحة الأسعار](https://www.eulerstream.com/pricing) لحدود الاتصالات المتزامنة في خطتك، لأن الحد يؤثر على عدد الغرف التي تقدر تشغّلها في نفس الوقت.

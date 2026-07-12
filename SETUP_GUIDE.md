# WhatsApp + Claude Smart Chatbot — Setup Guide

A chatbot that automatically answers your WhatsApp Business messages 24/7 using
Claude AI. It answers in the customer's language, uses only the facts you write
in `business_info.md`, remembers conversation context, and hands difficult
questions over to a human.

**Estimated monthly cost:** hosting ~€5 (Railway) + Claude API usage
(typically €3–15/month for a small business) + WhatsApp conversation fees from
Meta (first 1,000 service conversations/month are free). No per-seat platform
subscription.

**What you need before starting:** a Facebook account, a phone number for the
bot (it must NOT be actively used in the normal WhatsApp app — Cloud API takes
it over; a cheap prepaid SIM works fine), a bank card for Anthropic API credits.

---

## Step 1 — Create the Meta (WhatsApp) app · ~15 min

1. Go to <https://developers.facebook.com> → log in with Facebook → **My Apps → Create App**.
2. Choose **Business** type, name it (e.g. "MyShop Bot"), create it.
3. In the app dashboard find **WhatsApp → Set up**. This creates a test
   WhatsApp number automatically — you can test everything with it for free
   before connecting your real number.
4. Open **WhatsApp → API Setup** and note down:
   - **Phone number ID** → this is your `PHONE_NUMBER_ID`
   - **Temporary access token** (works 24 h — fine for testing; Step 5 makes a permanent one)
5. In **App settings → Basic**, note down the **App secret** → `APP_SECRET`.

## Step 2 — Get a Claude API key · ~5 min

1. Go to <https://console.anthropic.com> → create an account.
2. **Billing** → add a card and buy a small amount of credits (e.g. $5–10 to start).
3. **API Keys → Create Key** → copy it → this is your `ANTHROPIC_API_KEY`.

## Step 3 — Fill in your business knowledge · ~20 min

Edit `business_info.md`. This is the bot's brain — it will ONLY answer from
what is written there. Include: opening hours, full price list, delivery terms,
payment methods, return policy, and answers to your 10 most common customer
questions. The more complete this file, the smarter the bot.

You can write it in Lithuanian, English, or any language — the bot still
replies in whatever language the customer uses.

## Step 4 — Deploy to Railway · ~15 min

(Railway is the simplest; Render.com or Fly.io work the same way.)

1. Put this project folder into a GitHub repository (private is fine).
2. Go to <https://railway.app> → sign in with GitHub → **New Project →
   Deploy from GitHub repo** → pick your repo. Railway detects the Dockerfile
   and builds automatically.
3. In the service → **Variables**, add (values from `.env.example`):
   `WHATSAPP_TOKEN`, `PHONE_NUMBER_ID`, `APP_SECRET`, `VERIFY_TOKEN`
   (invent any secret string), `ANTHROPIC_API_KEY`.
4. **Settings → Networking → Generate Domain**. You get a URL like
   `https://myshop-bot.up.railway.app`. Opening it should show `{"status":"ok"}`.

> Note: the SQLite conversation memory resets on redeploys. For a small
> business this is fine; add a Railway Volume mounted at `/app` (and set
> `DB_PATH=/app/data/bot.db`) if you want it persistent.

## Step 5 — Connect the webhook · ~10 min

1. In Meta app dashboard: **WhatsApp → Configuration → Webhook → Edit**.
2. Callback URL: `https://YOUR-RAILWAY-URL/webhook`
   Verify token: the same `VERIFY_TOKEN` string you set in Railway.
3. Click **Verify and save** — it should turn green.
4. Under **Webhook fields**, subscribe to **messages**.

**Test now:** in Meta **API Setup** add your own phone as a recipient, send a
WhatsApp message to the test number — the bot should answer within a few
seconds. Check Railway → Deployments → Logs if anything fails.

## Step 6 — Make the token permanent · ~10 min

The token from Step 1 dies after 24 h. Permanent version:

1. <https://business.facebook.com> → **Settings → Users → System users** →
   **Add** → name it "bot", role Admin.
2. **Add assets** → assign your WhatsApp app with full control.
3. **Generate new token** → select your app → check permissions
   `whatsapp_business_messaging` and `whatsapp_business_management` →
   expiration **Never** → generate.
4. Put this token into Railway as `WHATSAPP_TOKEN` (replace the old one).

## Step 7 — Go live with your real number · ~15 min

1. Meta dashboard → **WhatsApp → API Setup → Add phone number**: enter your
   business number, verify by SMS/call. (If the number is currently in the
   WhatsApp/WhatsApp Business app, delete the account in the app first —
   Settings → Account → Delete account. Chats on the phone are lost, so export
   anything you need.)
2. Complete the business profile (name, category, logo).
3. Update `PHONE_NUMBER_ID` in Railway to the new number's ID.
4. To message customers who haven't written first, and to lift messaging
   limits, complete **Business verification** in Meta Business Manager
   (submit company documents; takes a few days). For *answering incoming
   messages only* — which is what this bot does — you don't need it initially.

## Daily use

- **Update the bot's knowledge:** edit `business_info.md` in GitHub → Railway
  redeploys automatically in ~1 min.
- **Take over a conversation manually:** the customer can be paused — send
  `#stop` from that customer's chat side (or add an owner-side pause; see
  below), and the bot stays silent for that chat until `#start`. Meanwhile you
  reply yourself from the WhatsApp Business app or Meta inbox.
- **The bot never invents facts** — anything not in `business_info.md` gets
  "a colleague will get back to you".
- **Costs monitoring:** console.anthropic.com shows Claude spend;
  Meta dashboard shows conversation counts.

## Rules & good practice (important)

- WhatsApp allows free-form replies only within **24 h of the customer's last
  message**. This bot only ever *replies*, so it always stays inside the
  window — that's why it works without approved templates.
- Meta requires an easy human-handoff path — the bot already offers a human
  for complaints and unknown questions. Keep it that way.
- Don't use the bot for marketing blasts without opt-in — that's how numbers
  get banned.

## Troubleshooting

| Problem | Fix |
|---|---|
| Webhook verify fails | `VERIFY_TOKEN` in Railway ≠ token typed in Meta; redeploy after changing variables |
| Bot receives but doesn't reply | Check Railway logs: 401 = bad/expired `WHATSAPP_TOKEN`; check Anthropic credits |
| Replies twice | Normally impossible (message-ID dedupe built in); check you don't run two instances |
| "(#131030) Recipient not in allowed list" | Test number can only message recipients added in API Setup — add your phone there |
| Answers in wrong language | Customer wrote very short/ambiguous text; the bot follows the customer's language by design |

## Next upgrades (ask Claude when ready)

- Owner notification (email/Telegram) when the bot hands off to a human
- Reading images/voice messages (Claude can analyze them)
- Booking/order integration (Google Calendar, e-shop API)
- Postgres instead of SQLite for full persistence

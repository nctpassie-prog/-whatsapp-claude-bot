# NCTPass garage bot — THIS FOLDER IS THE NCTPASS GARAGE ONLY

Do not mix this with the Headlights Repair bot (separate repo at
`C:\Users\headl\whatsup\headlights-bot`, separate Railway service, separate
WABA, separate diary). Owner: Tadas (non-technical — explain simply).

## What this is
WhatsApp + voice bot for **NCTPass** (nctpass.ie) — NCT pre-test inspection and
car repair garage, Unit 6, Old Quarry Campus, Blanchardstown, Dublin 15.
Hours Mon–Fri 9–18, Sat 9–14 (Saturday = general services only), Sun closed.

## Live infrastructure
- WhatsApp numbers (Chakra coexistence, plugin `80e84551-a3d5-4712-8124-f4db75a6bafa`):
  **085 777 7888** (WABA nctpass.ie `1713722639843344`, phone_id `1314437165075333`)
  and **086 667 7666** (WABA nctpass.ie `236685551234423`, phone_id `335852741443330`
  — the main line Dima & Vlad also use from the WhatsApp Business app).
- Railway: project **independent-playfulness**, service -whatsapp-claude-bot,
  domain `whatsapp-claude-bot-production-8b33.up.railway.app`, volume /data.
  GitHub `nctpassie-prog/-whatsapp-claude-bot` (push to main = auto-deploy;
  deploy = ~2 min bot blackout — avoid during business hours).
- Voice: Twilio **01 265 9310** → `/twiml/retell` → Retell agent
  **NCTPass Receptionist** `agent_2cd77ae0085ccacb85d3f9b257` (Mick Irish voice).
  086 and 085 forward-on-no-answer to it.
- Read-only admin key: `REVIEW_TOKEN=rev_1bef746188a3d79dab22929581ca0fa14ddb`
  (`/admin?token=...&action=status|day|customers|car|waiting|gaps|revenue|
  mechanicreport|sendmsg|botresume|...`, `/chats?token=...`).
- Gemini Flash answers all customers (Haiku fallback); Deepgram voice notes ON.
- Bookings → email onlinebookingnctpass@gmail.com + Google Calendar + Google
  Contacts auto-save (nctpass.ie@gmail.com). Alerts → Telegram bot "Nctpass alert"
  (chats 8905287298 + 1299744163; owner PRIVATE chat 1001948448 = @Tadasdiesel).

## Standing rules (owner's words — keep them)
- Booking order: problem → day → car+reg → name. Never ask for the phone number.
- Capacity 10/day Mon–Fri, Saturday 4 services-only, Sunday closed.
  (BOOKINGS_FROM gate dropped 2026-08-27 — any open day is bookable.)
- GOOD JOBS (book any day ahead): services/oil, NCT repairs/retest/fail,
  brakes, DPF, timing belt/chain, cambelt, clutch, flywheel. Everything else
  books only within AHEAD_ONLY_DAYS=2 of the date (honest AHEAD_ONLY_MSG, not
  "fully booked"). Pure diagnostics capped at DIAG_SLOTS_PER_DAY=2 (good jobs
  that include a diagnosis don't count). Re-confirmations of bookings already
  in the diary skip all gates (booking_already_in_diary).
- Prices always "from X plus VAT"; parts brand affects price; no discounts
  (one joke max, then soft handover). Free pre-NCT check WITH a service only.
- Wages/mechanic data NEVER to shared Telegram or WhatsApp — owner's private
  Telegram only. Weekly reports (gap report too) → private Telegram only.
- Humans always win: staff app reply silences the bot 24h; stalled staff chats
  get swept after 1h; `?action=botresume&date=<num>` resumes the bot NOW.
- Alert chases: customer chased after ALERT_CHASE_HOURS=2 if nobody replies.
- Missed WhatsApp calls get a "sorry we were busy — tell me here" text.
- Review funnel: rate-first (1–5), Google link only on 4–5, unhappy → private
  alert. visit_feedback template. Follow-ups: 2h nudge + next-day come_back_nudge
  template + weekly scoreboard.
- Owner's own number 0858182839 is blocklisted (bot never replies to him;
  his WhatsApp owner-commands are disabled on purpose).

## Key files
- `app.py` — the whole bot. `business_info.md` — knowledge base.
- `blocklist.txt` — numbers the bot never replies to.

Long history/details: memory file `nctpass-whatsapp-bot.md` in the
`C--Users-headl-whatsup` memory directory.

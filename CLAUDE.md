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
- HARD-JOB QUOTA (owner's rule 2026-08-27, replaced the old book-ahead system):
  ANY job books any open day up to 4 weeks ahead. HARD JOBS — diagnostics,
  injectors, turbo, clutch, engine work/noise, electrical, suspension, gearbox,
  wheel bearings (is_hard_job) — max HARD_JOBS_PER_DAY=4 per day and never on
  Saturday; the rest of each day is kept hunting easy service work. Hard-full
  day → honest HARD_FULL_MSG naming the nearest day with hard space.
  ?action=avail shows the exact calendar text the AI sees.
  Re-confirmations of bookings already in the diary skip all gates.
- Prices always "from X plus VAT"; parts brand affects price; no discounts
  (one joke max, then soft handover). Free pre-NCT check WITH a service only.
- Wages/mechanic data NEVER to shared Telegram or WhatsApp — owner's private
  Telegram only. Weekly reports (gap report too) → private Telegram only.
- Humans always win: staff app reply silences the bot 24h; stalled staff chats
  get swept after 1h; `?action=botresume&date=<num>` resumes the bot NOW.
- Alert chases: customer chased after ALERT_CHASE_HOURS=2 if nobody replies.
- STAFF "DONE" BUTTON (2026-09-02; owner: "we need only done" — NO separate
  claim/"I've got this" step): every needs-a-person Telegram alert carries one
  "✅ Done" inline button. Press → "✅ Done by <name> at HH:MM" on every copy,
  button gone. Not done and no staff reply after CLAIM_ESCALATE_MIN=30 →
  reposted (with the button) tagging CLAIM_MANAGER_MENTION (@Tadasdiesel);
  after CLAIM_OWNER_MIN=120 → owner's private chat; both 8am–8pm, once. A staff
  app reply or a booking closes it by itself. Only rows with `tg_msgs` (sent
  with the button) are on the clock. Names: Telegram first name unless mapped
  via `?action=claimname&phone=<tg user id>&need=<Name>` (owner 1001948448 →
  "Tadas" by default; ids appear in `?action=claimstatus` → tappers_seen).
  `telegram_button_loop` long-polls getUpdates (tgchat/tgpending fall back to
  the `tg_seen_chats` setting). Monday 8–10am Done scoreboard → private chat.
  Admin: `claimtest` (sample alert to the private chat only), `claimstatus`,
  `claimboard&need=<days>[&date=send]`. The claim_* code paths still exist
  (claimed_by etc.) but no button produces them.
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

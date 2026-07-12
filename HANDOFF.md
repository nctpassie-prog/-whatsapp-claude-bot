# HANDOFF — NCTPass WhatsApp bot (paste this into Claude Code)

You are continuing a nearly-finished project. Work step by step, test after
every step, and explain to Tadas (non-technical) in simple words what you do.

## Goal
A WhatsApp chatbot for NCTPass (car repair, Blanchardstown Dublin) that
auto-answers customer messages using Claude API. Everything is built and
deployed; ONLY the final wiring/testing remains.

## Current state (all verified working unless noted)
- Code repo: github.com/nctpassie-prog/-whatsapp-claude-bot (main branch)
  FastAPI app (app.py): Meta WhatsApp Cloud API webhook + Claude replies +
  SQLite memory + knowledge base in business_info.md
- Railway: TWO projects exist (duplicate deploy; delete the unused one later).
  Active domain: https://whatsapp-claude-bot-production-8b33.up.railway.app
  GET / returns {"status":"ok"} ✓ (bot is running)
  There was also an old domain ...-8539... on the other project.
- Meta app "Nctpassbot", app id 1585293593247330
  WhatsApp test number: +1 (555) 166-8314
  PHONE_NUMBER_ID=1228002437071752, WABA id 1463956005759773
  Allowed test recipient: +353858182839 (Tadas's phone)
  Test template message was delivered successfully ✓
- Anthropic API key works, credits purchased ✓ (model: claude-sonnet-5)

## The ONE remaining problem
Meta webhook verification fails ("callback URL or verify token couldn't be
validated"). Root cause is almost certainly: the Railway service serving
domain ...-8b33... is missing the environment variables (VERIFY_TOKEN etc.),
so GET /webhook returns 403 to Meta's challenge.

## Secrets (Tadas's own; treat carefully, suggest rotating when done)
- WHATSAPP_TOKEN (TEMPORARY, expires ~24h from 2026-07-12 19:00 Irish time;
  regenerate in Meta > use case > Step 1 Try it out if expired):
  <REDACTED — Tadas has it / regenerate in Meta Step 1>
- VERIFY_TOKEN: 7a7051b11271e90a26f9106189cc992d
- ANTHROPIC_API_KEY: <REDACTED — ask Tadas to paste it>
- Railway account token: <REDACTED — ask Tadas>
- GitHub PAT (classic, repo scope): <REDACTED — ask Tadas>
- APP_SECRET: not collected (optional; app works without signature check)

## Required Railway variables (raw editor format)
WHATSAPP_TOKEN=<see above>
PHONE_NUMBER_ID=1228002437071752
VERIFY_TOKEN=7a7051b11271e90a26f9106189cc992d
ANTHROPIC_API_KEY=<see above>
ANTHROPIC_MODEL=claude-sonnet-5

## Step plan
1. Install/verify railway CLI (npm i -g @railway/cli), login with
   RAILWAY_TOKEN env var (account token above; use `railway link` to pick the
   project that owns the -8b33 domain, service name contains
   "whatsapp-claude-bot").
2. Check variables: `railway variables`. Set any missing ones:
   `railway variables --set "VERIFY_TOKEN=7a7051b11271e90a26f9106189cc992d"` etc.
   Redeploy if needed: `railway redeploy`.
3. Test: curl "https://whatsapp-claude-bot-production-8b33.up.railway.app/webhook?hub.mode=subscribe&hub.verify_token=7a7051b11271e90a26f9106189cc992d&hub.challenge=test123"
   → must return exactly: test123
4. Configure Meta webhook via Graph API (or tell Tadas the 2 fields to paste):
   Callback URL: https://whatsapp-claude-bot-production-8b33.up.railway.app/webhook
   Verify token: 7a7051b11271e90a26f9106189cc992d
   Subscribe app to WABA: POST https://graph.facebook.com/v21.0/1463956005759773/subscribed_apps with Bearer WHATSAPP_TOKEN
   (Note: overriding the webhook URL app-wide may need app access token via
   /{app-id}/subscriptions — if that needs APP_SECRET, ask Tadas to paste the
   two fields in the Meta UI instead: use case > Step 2 Production setup >
   webhook section.)
5. Subscribe webhook field "messages" (UI: Webhook fields > Manage) if not done.
6. End-to-end test: Tadas sends a WhatsApp message from +353858182839 to
   +1 (555) 166-8314; bot must reply within seconds. Check Railway logs if not.
7. Cleanup: delete the unused duplicate Railway project; suggest Tadas
   regenerate GitHub PAT and Railway token later for security.

## Business knowledge (already in business_info.md in repo)
NCTPass, Unit 6 Old Quarry Campus, Blanchardstown, Dublin 15, D15 HX03.
Phone 086 667 7666. Mon-Fri 9-18, Sat 9-14. Labour from EUR 80/h, free
inspection with any service/repair, 12-month parts+labour guarantee,
pre-NCT checks, NCT repairs, servicing, tyres, brakes, diagnostics,
emissions, headlights. Bot: reply in customer's language, never invent
prices, collect booking info (car, problem, NCT fail sheet, preferred time,
name+phone), hand complaints to a human.

## Later roadmap (after test number works)
- Move to real business number (once Tadas decides; NOT 0866677666's
  WhatsApp until he's ready — Cloud API takes over the number's WhatsApp)
- Permanent WHATSAPP_TOKEN via Business Manager system user
- Add APP_SECRET signature verification
- Persistent volume for SQLite (or Postgres)

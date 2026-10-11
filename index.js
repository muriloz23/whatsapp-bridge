/**
 * Bridge WhatsApp ↔ site FaztFrammer — 100% self-hosted com Baileys.
 *
 * ENTRADA : msg no WhatsApp → POST {site}/api/whatsapp/webhook (formato Evolution)
 * SAÍDA   : poll {site}/api/whatsapp/outbox a cada 2s → sendMessage → ack
 *
 * Config (env ou config.json ao lado):
 *   SITE_URL          — ex.: https://faztframmer.vercel.app
 *   WA_WEBHOOK_SECRET — mesmo valor da env do site
 *
 * Primeira execução imprime o QR — escaneia com o WhatsApp Business
 * (Aparelhos conectados → Conectar aparelho). Sessão persiste em ./auth.
 */

const fs = require("fs");
const path = require("path");
const http = require("http");
const pino = require("pino");
const qrcode = require("qrcode-terminal");
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
} = require("@whiskeysockets/baileys");

let cfg = {};
try { cfg = require("./config.json"); } catch { }
const SITE = (process.env.SITE_URL || cfg.site_url || "https://faztframmer.vercel.app").replace(/\/+$/, "");
const SECRET = process.env.WA_WEBHOOK_SECRET || cfg.wa_webhook_secret || "";
const POLL_MS = 2000;

if (!SECRET) {
  console.error("Defina WA_WEBHOOK_SECRET (env ou config.json) — o mesmo do site.");
  process.exit(1);
}

const log = pino({ level: "warn" });

// Render/web service exige porta aberta — health check pra monitor (UptimeRobot
// pingando a cada 5min mantém o tier free acordado)
const PORT = process.env.PORT || 3000;
let connected = false;
let lastQr = null;
http
  .createServer(async (req, res) => {
    if (req.url === "/qr" && lastQr && !connected) {
      const QRCode = require("qrcode");
      const png = await QRCode.toBuffer(lastQr, { width: 320, margin: 2 });
      res.writeHead(200, { "Content-Type": "image/png" });
      return res.end(png);
    }
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, whatsapp: connected ? "connected" : "offline", qr: lastQr && !connected ? "/qr" : null }));
  })
  .listen(PORT, () => console.log("health check na porta " + PORT));

async function postWebhook(data) {
  const r = await fetch(`${SITE}/api/whatsapp/webhook?key=${SECRET}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ event: "messages.upsert", data }),
  });
  if (!r.ok) console.error("webhook", r.status);
}

async function drainOutbox(sock) {
  try {
    const r = await fetch(`${SITE}/api/whatsapp/outbox?key=${SECRET}`, { cache: "no-store" });
    if (!r.ok) return;
    const rows = await r.json();
    for (const row of rows) {
      try {
        if (row.kind === "image" && row.media_b64)
          await sock.sendMessage(row.jid, {
            image: Buffer.from(row.media_b64, "base64"),
            caption: row.caption || undefined,
          });
        else if (row.text)
          await sock.sendMessage(row.jid, { text: row.text });
        await fetch(`${SITE}/api/whatsapp/outbox?key=${SECRET}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ids: [row.id] }),
        });
      } catch (e) {
        console.error("falha ao enviar p/ " + row.jid + ":", e?.message || e);
      }
    }
  } catch { /* site fora — tenta no próximo tick */ }
}

async function start() {
  const { state, saveCreds } = await useMultiFileAuthState(path.join(__dirname, "auth"));
  const { version } = await fetchLatestBaileysVersion();
  const sock = makeWASocket({
    version,
    auth: state,
    logger: log,
    // evita o stall de "init queries" que conecta mas não entrega msgs —
    // o atendimento só precisa de msgs novas, não do histórico
    fireInitQueries: false,
    syncFullHistory: false,
    markOnlineOnConnect: false,
    shouldSyncHistoryMessage: () => false,
    generateHighQualityLinkPreview: false,
    connectTimeoutMs: 60_000,
    defaultQueryTimeoutMs: 60_000,
    keepAliveIntervalMs: 30_000,
  });

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", ({ connection, qr, lastDisconnect }) => {
    if (qr) {
      lastQr = qr;
      console.log("\n=== QR novo — abra /qr no navegador pra escanear ===\n");
      qrcode.generate(qr, { small: true });
    }
    if (connection === "open") { connected = true; lastQr = null; console.log("✓ WhatsApp conectado — atendimento ativo"); }
    if (connection === "close") {
      connected = false;
      const code = lastDisconnect?.error?.output?.statusCode;
      if (code === DisconnectReason.loggedOut) {
        console.error("Deslogado. Apague a pasta ./auth e rode de novo.");
        process.exit(1);
      }
      console.log("Conexão caiu (código " + code + ") — reconectando...");
      setTimeout(start, 3000);
    }
  });

  sock.ev.on("messages.upsert", ({ messages }) => {
    for (const m of messages) {
      if (!m?.key?.remoteJid) continue;
      console.log("msg recebida de", m.key.remoteJid, m.key.fromMe ? "(minha)" : "", "→ webhook");
      postWebhook(m).catch((e) => console.error("webhook:", e?.message || e));
    }
  });

  setInterval(() => drainOutbox(sock).catch(() => { }), POLL_MS);
}

start().catch((e) => {
  console.error(e);
  process.exit(1);
});

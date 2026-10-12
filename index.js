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
const { createClient } = require("@supabase/supabase-js");

let cfg = {};
try { cfg = require("./config.json"); } catch { }
const SITE = (process.env.SITE_URL || cfg.site_url || "https://faztframmer.vercel.app").replace(/\/+$/, "");
const SECRET = process.env.WA_WEBHOOK_SECRET || cfg.wa_webhook_secret || "";
const POLL_MS = 2000;
const AUTH_DIR = path.join(__dirname, "auth");

// Sessão persistida no Supabase Storage — sobrevive a redeploy no Render
// (filesystem efêmero). Sem essas envs cai no modo local-only (só pra dev).
const supa =
  (process.env.SUPABASE_URL || cfg.supabase_url) && (process.env.SUPABASE_SERVICE_ROLE_KEY || cfg.supabase_service_role_key)
    ? createClient(process.env.SUPABASE_URL || cfg.supabase_url, process.env.SUPABASE_SERVICE_ROLE_KEY || cfg.supabase_service_role_key)
    : null;
const AUTH_BUCKET = "wa-auth";

async function ensureAuthBucket() {
  const { error } = await supa.storage.getBucket(AUTH_BUCKET);
  if (error) {
    const { error: e2 } = await supa.storage.createBucket(AUTH_BUCKET, { public: false });
    if (e2) console.error("criar bucket auth:", e2.message);
  }
}

async function pullAuth() {
  fs.mkdirSync(AUTH_DIR, { recursive: true });
  const { data: files, error } = await supa.storage.from(AUTH_BUCKET).list("", { limit: 1000 });
  if (error) return console.error("listar auth:", error.message);
  for (const f of files || []) {
    const { data } = await supa.storage.from(AUTH_BUCKET).download(f.name);
    if (data) fs.writeFileSync(path.join(AUTH_DIR, f.name), Buffer.from(await data.arrayBuffer()));
  }
  if (files?.length) console.log("sessão restaurada do Supabase (" + files.length + " arquivos)");
}

async function pushAuth() {
  if (!fs.existsSync(AUTH_DIR)) return;
  for (const name of fs.readdirSync(AUTH_DIR)) {
    const p = path.join(AUTH_DIR, name);
    try {
      // Baileys remove pre-keys usadas — arquivo pode sumir entre readdir e stat
      if (!fs.statSync(p).isFile()) continue;
      const { error } = await supa.storage
        .from(AUTH_BUCKET)
        .upload(name, fs.readFileSync(p), { upsert: true, contentType: "application/octet-stream" });
      if (error) console.error("upload auth " + name + ":", error.message);
    } catch { /* arquivo sumiu — normal */ }
  }
}

let syncTimer = null;
function queueAuthSync() {
  if (!supa || syncTimer) return;
  syncTimer = setTimeout(() => {
    syncTimer = null;
    pushAuth().catch((e) => console.error("sync auth:", e?.message || e));
  }, 3000);
}

if (!SECRET) {
  console.error("Defina WA_WEBHOOK_SECRET (env ou config.json) — o mesmo do site.");
  process.exit(1);
}

const log = pino({ level: process.env.WA_LOG_LEVEL || "warn" });

// Render/web service exige porta aberta — health check pra monitor (UptimeRobot
// pingando a cada 5min mantém o tier free acordado)
const PORT = process.env.PORT || 3000;
let connected = false;
let lastQr = null;
http
  .createServer(async (req, res) => {
    const url = new URL(req.url, "http://x");
    // zera sessão (local + bucket) quando as chaves ficam corrompidas —
    // o processo morre, o Render reinicia e o /qr pede pareamento novo
    if (url.pathname === "/reset-auth") {
      if (url.searchParams.get("key") !== SECRET)
        return res.writeHead(401).end("unauthorized");
      res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("sessão zerada — o serviço vai reiniciar e pedir QR novo em /qr");
      setTimeout(async () => {
        try {
          if (supa) {
            const { data: files } = await supa.storage.from(AUTH_BUCKET).list("", { limit: 1000 });
            if (files?.length)
              await supa.storage.from(AUTH_BUCKET).remove(files.map((f) => f.name));
          }
          fs.rmSync(AUTH_DIR, { recursive: true, force: true });
        } catch { }
        process.exit(0); // Render reinicia sozinho
      }, 500);
      return;
    }
    if (url.pathname === "/qr" && lastQr && !connected) {
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

// PN → LID aprendido das mensagens recebidas (remoteJid @lid + senderPn).
// A sessão Signal do contato vive no @lid — mandar pro PN sem o mapeamento
// faz o Baileys criar sessão nova e falhar com "reading 'id'".
const lidFor = new Map();

// resolve o JID de envio: se a sessão do contato vive num @lid, manda pra lá;
// @lid vira PN quando o mapeamento existe (a msg cai na conversa certa)
async function resolveSendJid(sock, jid) {
  if (jid.endsWith("@lid")) {
    try {
      const lid = jid.split("@")[0].split(":")[0];
      const pn = await sock.signalRepository?.lidMapping?.getPNForLID?.(lid);
      const n = typeof pn === "string" ? pn.split("@")[0].split(":")[0] : pn?.toString?.().split("@")[0].split(":")[0];
      if (n && /^\d{10,15}$/.test(n)) return `${n}@s.whatsapp.net`;
    } catch { }
    return jid;
  }
  const pnNum = jid.split("@")[0];
  if (lidFor.has(pnNum)) return `${lidFor.get(pnNum)}@lid`;
  try {
    const lid = await sock.signalRepository?.lidMapping?.getLIDForPN?.(pnNum);
    const n = typeof lid === "string" ? lid.split("@")[0].split(":")[0]
      : lid?.lid ? String(lid.lid).split("@")[0].split(":")[0]
      : lid?.toString?.().split("@")[0].split(":")[0];
    if (n && /^\d{10,15}$/.test(n)) { lidFor.set(pnNum, n); return `${n}@lid`; }
  } catch { }
  return jid;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Apaga a sessão Signal corrompida do contato: store do Baileys (se a API
// existir) + arquivos session-*.{json} do auth dir + bucket (pra não voltar
// corrompida num redeploy). Próxima msg renegocia do zero via prekey.
async function resetContactSession(sock, jid) {
  const num = jid.split("@")[0].split(":")[0];
  try { await sock.signalRepository?.deleteSession?.(jid); } catch { }
  try {
    for (const f of fs.readdirSync(AUTH_DIR)) {
      if (f.startsWith("session-") && f.includes(num))
        try { fs.rmSync(path.join(AUTH_DIR, f), { force: true }); } catch { }
    }
  } catch { }
  if (supa) {
    try {
      const { data: files } = await supa.storage.from(AUTH_BUCKET).list("", { limit: 1000 });
      const bad = (files || []).filter((f) => f.name.startsWith("session-") && f.name.includes(num)).map((f) => f.name);
      if (bad.length) await supa.storage.from(AUTH_BUCKET).remove(bad);
    } catch { }
  }
  console.log("sessão resetada p/ " + jid);
}

// envio "humano": subscribe (negocia a sessão E2E antes) → digitando → delay
// proporcional ao tamanho → envia → pausa. Reduz "Aguardando mensagem" e ban.
async function sendHuman(sock, jid, payload) {
  const target = await resolveSendJid(sock, jid);
  // força criar a sessão Signal com o contato ANTES da msg — é isso que mata
  // o "Aguardando mensagem" no celular de quem recebe
  try { await sock.assertSessions?.([target], false); } catch { }
  try { await sock.presenceSubscribe(target); } catch { }
  try { await sock.sendPresenceUpdate("composing", target); } catch { }
  const len = payload.text?.length ?? payload.caption?.length ?? 180;
  await sleep(Math.min(3500, 600 + len * 20));
  try {
    await sock.sendMessage(target, payload);
  } catch (e) {
    // sessão Signal corrompida (Bad MAC / "no sessions" / "reading 'id'") —
    // apaga a sessão quebrada, renegocia via prekey bundle e tenta 1x de novo
    console.error("send falhou (" + (e?.message || e) + ") — resetando sessão e retentando");
    await resetContactSession(sock, target);
    try { await sock.assertSessions?.([target], true); } catch { }
    await sleep(400);
    await sock.sendMessage(target, payload);
  }
  try { await sock.sendPresenceUpdate("paused", target); } catch { }
  return target;
}

async function drainOutbox(sock) {
  try {
    const r = await fetch(`${SITE}/api/whatsapp/outbox?key=${SECRET}`, { cache: "no-store" });
    if (!r.ok) {
      if (!drainOutbox.warned) { console.error("outbox", r.status, "— confira WA_WEBHOOK_SECRET no Vercel"); drainOutbox.warned = true; }
      return;
    }
    const rows = await r.json();
    for (const row of rows) {
      try {
        let target;
        if (row.kind === "image" && row.media_b64)
          target = await sendHuman(sock, row.jid, {
            image: Buffer.from(row.media_b64, "base64"),
            caption: row.caption || undefined,
          });
        else if (row.text)
          target = await sendHuman(sock, row.jid, { text: row.text });
        else continue;
        await fetch(`${SITE}/api/whatsapp/outbox?key=${SECRET}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ids: [row.id] }),
        });
      } catch (e) {
        console.error("falha ao enviar p/ " + row.jid + ":", e?.message || e);
      }
      await sleep(400); // respiro entre mensagens — não metralha
    }
  } catch { /* site fora — tenta no próximo tick */ }
}

async function start() {
  if (supa) {
    try {
      await ensureAuthBucket();
      await pullAuth();
    } catch (e) {
      console.error("restaurar sessão:", e?.message || e);
    }
  }
  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const { version } = await fetchLatestBaileysVersion();
  const sock = makeWASocket({
    version,
    auth: state,
    logger: log,
    // evita o stall de "init queries" que conecta mas não entrega msgs —
    // o atendimento só precisa de msgs novas, não do histórico
    fireInitQueries: false,
    syncFullHistory: false,
    markOnlineOnConnect: true,
    shouldSyncHistoryMessage: () => true,
    generateHighQualityLinkPreview: false,
    connectTimeoutMs: 60_000,
    defaultQueryTimeoutMs: 60_000,
    keepAliveIntervalMs: 30_000,
  });

  sock.ev.on("creds.update", async () => {
    await saveCreds();
    queueAuthSync();
  });

  sock.ev.on("connection.update", async ({ connection, qr, lastDisconnect }) => {
    if (qr) {
      lastQr = qr;
      console.log("\n=== QR novo — abra /qr no navegador pra escanear ===\n");
      qrcode.generate(qr, { small: true });
    }
    if (connection === "open") { connected = true; lastQr = null; console.log("✓ WhatsApp conectado — atendimento ativo"); queueAuthSync(); }
    if (connection === "close") {
      connected = false;
      const code = lastDisconnect?.error?.output?.statusCode;
      if (code === DisconnectReason.loggedOut) {
        console.error("Deslogado — limpando sessão (local + bucket) pra pedir QR novo...");
        try {
          if (supa) {
            const { data: files } = await supa.storage.from(AUTH_BUCKET).list("", { limit: 1000 });
            if (files?.length)
              await supa.storage.from(AUTH_BUCKET).remove(files.map((f) => f.name));
          }
          fs.rmSync(AUTH_DIR, { recursive: true, force: true });
        } catch { }
        process.exit(1); // Render reinicia → bucket vazio → QR novo
      }
      console.log("Conexão caiu (código " + code + ") — reconectando...");
      setTimeout(start, 3000);
    }
  });

  const startedAt = Math.floor(Date.now() / 1000);
  const ppCache = new Map();
  const ppFor = async (jid) => {
    if (ppCache.has(jid)) return ppCache.get(jid);
    let url = null;
    try { url = await sock.profilePictureUrl(jid, "image"); } catch { }
    ppCache.set(jid, url);
    return url;
  };

  sock.ev.on("messages.upsert", ({ messages, type }) => {
    console.log("messages.upsert type=" + type + " count=" + (messages?.length || 0));
    for (const m of messages || []) {
      if (!m?.key?.remoteJid) continue;
      // aprende LID ↔ PN: a sessão cripto vive no @lid — guarda pra responder lá
      if (m.key.remoteJid.endsWith("@lid")) {
        const lid = m.key.remoteJid.split("@")[0].split(":")[0];
        const pn = (m.key.remoteJidAlt ?? m.key.senderPn ?? "").split("@")[0].split(":")[0];
        if (lid && pn && /^\d{10,15}$/.test(pn)) lidFor.set(pn, lid);
      }
      console.log(
        "msg recebida de", m.key.remoteJid,
        m.key.fromMe ? "(minha)" : "",
        "tipo:", Object.keys(m.message || {}).join("+") || "sem-message",
        "→ webhook"
      );
      (m.key.fromMe ? Promise.resolve(null) : ppFor(m.key.remoteJidAlt ?? m.key.remoteJid))
        .then((ppUrl) => postWebhook({ ...m, ppUrl }))
        .catch((e) => console.error("webhook:", e?.message || e));
    }
  });

  // msgs que chegaram enquanto o socket estava caindo/reconectando vêm por
  // history.set em vez de upsert — repassa as recentes (o webhook deduplica por waId)
  sock.ev.on("messaging-history.set", ({ messages }) => {
    const fresh = (messages || []).filter(
      (m) => m?.key?.remoteJid && !m.key.fromMe && (m.messageTimestamp ?? 0) >= startedAt
    );
    console.log("history.set count=" + (messages?.length || 0) + " novas=" + fresh.length);
    for (const m of fresh) {
      console.log("msg recebida (sync) de", m.key.remoteJid, "→ webhook");
      postWebhook(m).catch((e) => console.error("webhook:", e?.message || e));
    }
  });

  setInterval(() => drainOutbox(sock).catch(() => { }), POLL_MS);
  // chaves de sessão mudam durante conversas — sync periódico garante que
  // um redeploy não volte com chaves velhas e falhe a descriptografia
  if (supa) setInterval(queueAuthSync, 60_000);
}

start().catch((e) => {
  console.error(e);
  process.exit(1);
});

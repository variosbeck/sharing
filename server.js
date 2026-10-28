const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_PEOPLE = 8; // malha P2P: cada pessoa envia para todas as outras
const PERM_KEYS = ['share', 'cam', 'chat']; // cada uma: 'all' ou 'admins'

// ---------- ICE / TURN ----------
// TURN da Cloudflare: defina CF_TURN_KEY_ID e CF_TURN_API_TOKEN no ambiente.
// Alternativa (outro provedor): TURN_URL, TURN_USER e TURN_PASS.
const STUN = { urls: 'stun:stun.l.google.com:19302' };
const CF_TTL = 86400; // validade da credencial (s)
let cfCache = { servers: null, exp: 0 };
async function getIce() {
  const keyId = process.env.CF_TURN_KEY_ID, token = process.env.CF_TURN_API_TOKEN;
  if (keyId && token) {
    if (cfCache.servers && Date.now() < cfCache.exp) return cfCache.servers;
    try {
      const r = await fetch('https://rtc.live.cloudflare.com/v1/turn/keys/' + keyId + '/credentials/generate-ice-servers', {
        method: 'POST',
        headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
        body: JSON.stringify({ ttl: CF_TTL }),
      });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const data = await r.json();
      if (!Array.isArray(data.iceServers) || !data.iceServers.length) throw new Error('resposta sem iceServers');
      cfCache = { servers: data.iceServers, exp: Date.now() + (CF_TTL / 2) * 1000 }; // renova na metade da validade
      return cfCache.servers;
    } catch (e) {
      console.error('Falha ao gerar credencial TURN da Cloudflare:', e.message);
      if (cfCache.servers) return cfCache.servers;
    }
  }
  const list = [STUN];
  if (process.env.TURN_URL) list.push({ urls: process.env.TURN_URL.split(','), username: process.env.TURN_USER, credential: process.env.TURN_PASS });
  return list;
}

const server = http.createServer((req, res) => {
  if (req.url === '/rooms') {
    const list = [...rooms.entries()]
      .filter(([, r]) => r.public && r.peers.size)
      .map(([room, r]) => ({ room, people: r.peers.size, cat: r.cat, desc: r.desc }))
      .sort((a, b) => b.people - a.people).slice(0, 20);
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify(list));
  }
  if (req.url === '/ice') {
    return getIce().then((iceServers) => {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
      res.end(JSON.stringify({ iceServers }));
    });
  }
  fs.readFile(path.join(__dirname, 'public', 'index.html'), (err, data) => {
    res.writeHead(err ? 500 : 200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server, path: '/ws' });
const CATS = ['Gameplay', 'Conversa', 'Música', 'Filmes e séries', 'Estudos', 'Trabalho', 'Esportes', 'Programação', 'Arte e desenho', 'Outros'];
const rooms = new Map(); // sala -> { pass, ownerTok, admins, bans, limit, perms, peers: Map(id -> ws) }
let counter = 0;
const out = (ws, o) => ws.readyState === 1 && ws.send(JSON.stringify(o));

// ---------- papéis ----------
// O papel vem de um token guardado no navegador, então quem recarrega a página continua dono/admin.
const roleOf = (r, ws) => (ws.tok === r.ownerTok ? 'owner' : r.admins.has(ws.tok) ? 'admin' : 'member');
const isMod = (r, ws) => roleOf(r, ws) !== 'member';
const canDo = (r, ws, key) => r.perms[key] === 'all' || isMod(r, ws);
function stateFor(r, ws) {
  const roles = {};
  r.peers.forEach((p) => (roles[p.id] = roleOf(r, p)));
  const st = { type: 'state', roles, limit: r.limit, perms: r.perms };
  if (isMod(r, ws)) st.bans = [...r.bans.entries()].map(([key, b]) => ({ key, name: b.name }));
  return st;
}
const pushState = (r) => r.peers.forEach((p) => out(p, stateFor(r, p)));
const isBanned = (r, ws) => [...r.bans.values()].some((b) => b.tok === ws.tok); // só pelo token do navegador: banir por IP pegaria quem divide a mesma rede

wss.on('connection', (ws) => {
  ws.id = (++counter).toString(36) + Math.random().toString(36).slice(2, 6);

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'join' && !ws.room) {
      const room = String(m.room || '');
      if (!/^[\w-]{1,40}$/.test(room) || ['ice', 'rooms', 'ws'].includes(room)) return ws.close();
      ws.tok = String(m.tok || '').slice(0, 64) || ws.id;
      const pass = String(m.pass || '').slice(0, 40);
      let r = rooms.get(room);
      if (r && isBanned(r, ws)) { out(ws, { type: 'banned' }); return ws.close(); }
      if (r && r.pass && r.pass !== pass) { out(ws, { type: pass ? 'badpass' : 'needpass' }); return ws.close(); }
      const full = r && r.peers.size >= (ws.tok === r.ownerTok ? MAX_PEOPLE : r.limit); // o dono sempre consegue voltar
      if (full) { out(ws, { type: 'full', limit: r.limit }); return ws.close(); }
      if (!r) {
        rooms.set(room, (r = {
          pass, peers: new Map(), history: [], public: !pass && !!m.public,
          cat: CATS.includes(m.cat) ? m.cat : '', desc: String(m.desc || '').trim().slice(0, 80),
          ownerTok: ws.tok, admins: new Set(), bans: new Map(), banSeq: 0,
          limit: MAX_PEOPLE, perms: { share: 'all', cam: 'all', chat: 'all' },
        })); // quem cria vira o dono e define a senha
      }
      ws.room = room;
      ws.name = String(m.name || '').slice(0, 24) || 'Participante';
      out(ws, { type: 'hello', id: ws.id, peers: [...r.peers.values()].map((p) => ({ id: p.id, name: p.name })), history: r.history });
      r.peers.forEach((p) => out(p, { type: 'join', id: ws.id, name: ws.name }));
      r.peers.set(ws.id, ws);
      pushState(r);
    } else if (m.type === 'signal' && ws.room) {
      const t = rooms.get(ws.room)?.peers.get(m.to);
      if (t) out(t, { type: 'signal', from: ws.id, data: m.data });
    } else if (m.type === 'chat' && ws.room) {
      const r = rooms.get(ws.room);
      const text = String(m.text || '').trim().slice(0, 500);
      if (!r || !text) return;
      if (!canDo(r, ws, 'chat')) return out(ws, { type: 'denied', what: 'chat' });
      const msg = { type: 'chat', id: ws.id, name: ws.name, text, ts: Date.now() };
      r.history.push({ name: ws.name, text, ts: msg.ts });
      if (r.history.length > 50) r.history.shift();
      r.peers.forEach((p) => out(p, msg));
    } else if (m.type === 'typing' && ws.room) {
      const r = rooms.get(ws.room);
      if (r && canDo(r, ws, 'chat')) r.peers.forEach((p) => p !== ws && out(p, { type: 'typing', name: ws.name }));
    } else if (m.type === 'admin' && ws.room) {
      const r = rooms.get(ws.room);
      if (!r || !isMod(r, ws)) return;
      const mine = roleOf(r, ws), t = r.peers.get(m.target);
      if (m.act === 'kick' || m.act === 'ban') {
        if (!t || t === ws) return;
        const tr = roleOf(r, t);
        if (tr === 'owner' || (tr === 'admin' && mine !== 'owner')) return; // admin não mexe em admin nem no dono
        if (m.act === 'ban') {
          r.admins.delete(t.tok);
          r.bans.set('b' + ++r.banSeq, { name: t.name, tok: t.tok });
        }
        out(t, { type: m.act === 'ban' ? 'banned' : 'kicked' });
        t.close(); // o evento 'close' avisa a sala e atualiza o estado
        if (m.act === 'ban') pushState(r);
      } else if (m.act === 'promote' || m.act === 'demote') {
        if (mine !== 'owner' || !t || t === ws) return;
        if (m.act === 'promote') r.admins.add(t.tok); else r.admins.delete(t.tok);
        pushState(r);
      } else if (m.act === 'set') {
        const lim = Math.round(Number(m.limit));
        if (lim >= 2 && lim <= MAX_PEOPLE) r.limit = lim; // abaixo de quem já está: só bloqueia novas entradas
        PERM_KEYS.forEach((k) => { const v = m.perms && m.perms[k]; if (v === 'all' || v === 'admins') r.perms[k] = v; });
        pushState(r);
      } else if (m.act === 'unban') {
        if (r.bans.delete(String(m.key))) pushState(r);
      }
    }
  });

  ws.on('close', () => {
    const r = rooms.get(ws.room);
    if (!r || r.peers.get(ws.id) !== ws) return;
    r.peers.delete(ws.id);
    r.peers.forEach((p) => out(p, { type: 'leave', id: ws.id }));
    if (!r.peers.size) rooms.delete(ws.room);
    else pushState(r);
  });
});

server.listen(PORT, () => console.log('ScreenCast rodando na porta ' + PORT));
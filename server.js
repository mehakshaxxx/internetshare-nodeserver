/**
 * InternetShare Relay Server
 *
 * Single Node.js process that does three things:
 *   1. Issues short numeric "device codes" to clients on first connect.
 *   2. Pairs a receiver to a sharer when receiver enters sharer's code.
 *   3. Forwards binary data frames between the two peers (sharer ↔ receiver).
 *
 * Wire format (JSON for control, length-prefixed binary for data):
 *   Control frames are text JSON messages.
 *   Data frames are binary, first byte = stream-id high, second = stream-id low,
 *   third = opcode (0=open,1=data,2=close), rest = payload.
 *
 * This server is intentionally protocol-agnostic about what the bytes mean —
 * the receiver app encodes its TUN packets / proxied TCP streams here, the
 * sharer app decodes and forwards to the actual internet, and back.
 */

'use strict';

const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const HOST = process.env.HOST || '0.0.0.0';

// ──────────────────────────────────────────────────────────────────────────
// In-memory state. For a single-instance deployment this is fine.
// ──────────────────────────────────────────────────────────────────────────

/** code (6-digit string) → { socket, deviceId, mode, pairedWith } */
const devices = new Map();
/** sessionId → { sharer: ws|null, receiver: ws|null, startedAt, bytesSharer, bytesReceiver, limits } */
const sessions = new Map();
const CONTROL_REATTACH_GRACE_MS = Number(process.env.CONTROL_REATTACH_GRACE_MS || 30_000);

function genCode() {
  // 6-digit numeric code, leading zeros allowed → 1 000 000 combinations.
  // Collision retry below keeps it correct.
  for (let attempt = 0; attempt < 20; attempt++) {
    const code = String(crypto.randomInt(0, 1_000_000)).padStart(6, '0');
    if (!devices.has(code)) return code;
  }
  throw new Error('Could not allocate unique code (server is full)');
}

function genSessionId() {
  return crypto.randomBytes(8).toString('hex');
}

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function sendBinary(ws, buf) {
  if (ws && ws.readyState === ws.OPEN) {
    ws.send(buf, { binary: true });
  }
}

function sessionPeer(session, role) {
  return role === 'sharer' ? session.receiver : session.sharer;
}

function endSession(sessionId, reason) {
  const session = sessions.get(sessionId);
  if (!session) return;
  for (const sock of [session.sharer, session.receiver]) {
    send(sock, { type: 'session_ended', reason });
    if (sock) sock.sessionId = null;
  }
  for (const sock of [session.sharerData, session.receiverData]) {
    if (sock) {
      try { sock.close(1000, reason); } catch {}
    }
  }
  if (session.graceTimer) clearTimeout(session.graceTimer);
  sessions.delete(sessionId);
}

function scheduleControlGrace(ws) {
  const sessionId = ws.sessionId;
  if (!sessionId) return;
  const session = sessions.get(sessionId);
  if (!session) return;

  const role = ws.role;
  if (role === 'sharer' && session.sharer === ws) session.sharer = null;
  if (role === 'receiver' && session.receiver === ws) session.receiver = null;

  send(sessionPeer(session, role), {
    type: 'peer_disconnected',
    role,
    graceMs: CONTROL_REATTACH_GRACE_MS,
  });

  if (session.graceTimer) clearTimeout(session.graceTimer);
  session.graceTimer = setTimeout(() => {
    const current = sessions.get(sessionId);
    if (!current) return;
    if (!current.sharer || !current.receiver) {
      endSession(sessionId, 'peer_disconnected_timeout');
    }
  }, CONTROL_REATTACH_GRACE_MS);
}

function resumeControlSocket(ws, msg) {
  const sessionId = String(msg.sessionId || '');
  const role = msg.role;
  const deviceId = msg.deviceId;
  const session = sessions.get(sessionId);
  if (!session || (role !== 'sharer' && role !== 'receiver') || !deviceId) {
    send(ws, { type: 'resume_failed', reason: 'bad_session' });
    return;
  }
  const expectedDeviceId = role === 'sharer' ? session.sharerDeviceId : session.receiverDeviceId;
  if (expectedDeviceId !== deviceId) {
    send(ws, { type: 'resume_failed', reason: 'device_mismatch' });
    return;
  }

  ws.sessionId = sessionId;
  ws.role = role;
  ws.deviceId = deviceId;
  if (role === 'sharer') session.sharer = ws;
  else session.receiver = ws;

  if (session.graceTimer) {
    clearTimeout(session.graceTimer);
    session.graceTimer = null;
  }

  send(ws, {
    type: 'session_resumed',
    sessionId,
    role,
    startedAt: session.startedAt,
    limits: session.limits,
  });
  send(sessionPeer(session, role), { type: 'peer_reconnected', role });
}

// ──────────────────────────────────────────────────────────────────────────
// HTTP server (for health checks behind cPanel / nginx)
// ──────────────────────────────────────────────────────────────────────────

const httpServer = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(
      JSON.stringify({
        ok: true,
        devices: devices.size,
        sessions: sessions.size,
        uptime: process.uptime(),
      }),
    );
    return;
  }
  res.writeHead(404);
  res.end('Not Found');
});

// ──────────────────────────────────────────────────────────────────────────
// WebSocket server
// ──────────────────────────────────────────────────────────────────────────

const wss = new WebSocketServer({ server: httpServer, maxPayload: 4 * 1024 * 1024 });

wss.on('connection', (ws, req) => {
  ws.isAlive = true;
  ws.code = null;
  ws.role = null; // 'sharer' | 'receiver' | null
  ws.sessionId = null;
  ws.pairedWith = null;
  // Native data channels open a SECOND socket that uses native_attach to
  // bind to an already-established session. We mark those sockets so
  // handleBinary knows to route between native peers instead of JS peers.
  ws.isNativeChannel = false;

  console.log(`[conn] ${req.socket.remoteAddress} connected`);

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (raw, isBinary) => {
    if (isBinary) {
      handleBinary(ws, raw);
    } else {
      let msg;
      try {
        msg = JSON.parse(raw.toString('utf8'));
      } catch {
        send(ws, { type: 'error', error: 'invalid_json' });
        return;
      }
      handleControl(ws, msg);
    }
  });

  ws.on('close', () => {
    console.log(`[conn] disconnected (code=${ws.code} role=${ws.role} native=${ws.isNativeChannel})`);
    if (ws.code) devices.delete(ws.code);
    if (ws.sessionId) {
      const session = sessions.get(ws.sessionId);
      if (session) {
        if (ws.isNativeChannel) {
          // Just one half of the native data plane dropped. Don't tear down
          // the whole session — JS control sockets still own session lifetime.
          if (session.sharerData === ws) session.sharerData = null;
          if (session.receiverData === ws) session.receiverData = null;
          // Tell the peer's native channel its counterpart is gone so it can
          // stop pumping bytes into the void.
          const peerData = ws.role === 'sharer' ? session.receiverData : session.sharerData;
          send(peerData, { type: 'peer_native_disconnected' });
        } else {
          // JS control socket dropped — session is over.
          scheduleControlGrace(ws);
        }
      }
    }
  });

  ws.on('error', err => console.error('[ws error]', err.message));
});

function handleControl(ws, msg) {
  switch (msg.type) {
    case 'hello': {
      // Client registers itself; we hand back a fresh 6-digit code.
      // msg.deviceId is a UUID generated by the app on first launch and
      // persisted locally — useful for showing "remember this device" later.
      const code = genCode();
      ws.code = code;
      ws.deviceId = msg.deviceId || null;
      devices.set(code, ws);
      send(ws, { type: 'hello_ack', code, serverTime: Date.now() });
      console.log(`[hello] device=${ws.deviceId} -> code=${code}`);
      break;
    }

    case 'connect_request': {
      // Receiver asks to connect to sharer by code.
      const target = devices.get(String(msg.code || '').trim());
      if (!target) {
        send(ws, { type: 'connect_failed', reason: 'code_not_found' });
        return;
      }
      if (target === ws) {
        send(ws, { type: 'connect_failed', reason: 'cannot_connect_self' });
        return;
      }
      if (target.sessionId) {
        send(ws, { type: 'connect_failed', reason: 'target_busy' });
        return;
      }
      // Forward request to sharer for approval. Sharer decides duration / data
      // limits in their own UI before responding.
      const pendingId = crypto.randomBytes(8).toString('hex');
      ws.pendingRequestId = pendingId;
      target.pendingFrom = ws;
      target.pendingRequestId = pendingId;
      send(target, {
        type: 'incoming_request',
        requestId: pendingId,
        fromCode: ws.code,
      });
      send(ws, { type: 'connect_pending' });
      break;
    }

    case 'approve_request': {
      // Sharer accepts. msg = { requestId, durationMs, dataLimitBytes }
      const receiver = ws.pendingFrom;
      if (!receiver || ws.pendingRequestId !== msg.requestId) {
        send(ws, { type: 'error', error: 'no_pending_request' });
        return;
      }
      const sessionId = genSessionId();
      const session = {
        sharer: ws,            // JS control socket (sharer)
        receiver,              // JS control socket (receiver)
        sharerData: null,      // native data channel (sharer) — bound via native_attach
        receiverData: null,    // native data channel (receiver) — bound via native_attach
        startedAt: Date.now(),
        bytesSharer: 0,
        bytesReceiver: 0,
        sharerDeviceId: ws.deviceId,
        receiverDeviceId: receiver.deviceId,
        graceTimer: null,
        limits: {
          durationMs: Number(msg.durationMs) || 0,
          dataLimitBytes: Number(msg.dataLimitBytes) || 0,
        },
      };
      sessions.set(sessionId, session);
      ws.sessionId = sessionId;
      ws.role = 'sharer';
      receiver.sessionId = sessionId;
      receiver.role = 'receiver';
      ws.pendingFrom = null;
      ws.pendingRequestId = null;
      receiver.pendingRequestId = null;

      const payload = {
        sessionId,
        startedAt: session.startedAt,
        limits: session.limits,
      };
      send(ws, { type: 'session_started', ...payload, role: 'sharer' });
      send(receiver, { type: 'session_started', ...payload, role: 'receiver' });
      console.log(`[session] ${sessionId} sharer=${ws.code} receiver=${receiver.code}`);
      break;
    }

    case 'reject_request': {
      const receiver = ws.pendingFrom;
      if (receiver) {
        send(receiver, { type: 'connect_failed', reason: 'rejected' });
        receiver.pendingRequestId = null;
      }
      ws.pendingFrom = null;
      ws.pendingRequestId = null;
      break;
    }

    case 'end_session': {
      if (!ws.sessionId) return;
      endSession(ws.sessionId, msg.reason || 'peer_ended');
      break;
    }

    case 'stats_update': {
      // Optional: clients can push their local byte counters for the admin
      // dashboard. Not load-bearing; transport-level counting also happens
      // in handleBinary below.
      if (!ws.sessionId) return;
      const session = sessions.get(ws.sessionId);
      if (!session) return;
      const peer = session.sharer === ws ? session.receiver : session.sharer;
      send(peer, { type: 'peer_stats', ...msg.stats });
      break;
    }

    case 'native_attach': {
      // The native VPN/exit services open their own WebSocket and bind it
      // to an already-established session by deviceId + role. After this,
      // every binary frame on this socket is forwarded to the OPPOSITE
      // native channel on the same session.
      const deviceId = msg.deviceId;
      const role = msg.role;
      if (!deviceId || (role !== 'sharer' && role !== 'receiver')) {
        send(ws, { type: 'native_attach_failed', reason: 'bad_args' });
        return;
      }
      let foundSid = null;
      let foundSession = null;
      for (const [sid, s] of sessions) {
        const expectedDeviceId = role === 'sharer' ? s.sharerDeviceId : s.receiverDeviceId;
        if (expectedDeviceId === deviceId) {
          foundSid = sid;
          foundSession = s;
          break;
        }
      }
      if (!foundSession) {
        send(ws, { type: 'native_attach_failed', reason: 'no_session_for_device' });
        return;
      }
      ws.sessionId = foundSid;
      ws.role = role;
      ws.deviceId = deviceId;
      ws.isNativeChannel = true;
      if (role === 'sharer') foundSession.sharerData = ws;
      else foundSession.receiverData = ws;
      send(ws, { type: 'native_attach_ack', sessionId: foundSid });
      console.log(`[native_attach] device=${deviceId} role=${role} session=${foundSid}`);
      break;
    }

    case 'resume_session':
      resumeControlSocket(ws, msg);
      break;

    case 'ping':
      send(ws, { type: 'pong', t: Date.now() });
      break;

    default:
      send(ws, { type: 'error', error: 'unknown_type', received: msg.type });
  }
}

function handleBinary(ws, buf) {
  // Binary data only flows once a session is open. We blindly forward to the
  // paired peer and count bytes for limit enforcement.
  if (!ws.sessionId) return;
  const session = sessions.get(ws.sessionId);
  if (!session) return;

  // Pick the peer:
  //  - native data channel  → forward to the OTHER native data channel
  //  - JS control socket    → forward to the OTHER JS control socket (legacy)
  let peer;
  if (ws.isNativeChannel) {
    peer = ws.role === 'sharer' ? session.receiverData : session.sharerData;
  } else {
    peer = session.sharer === ws ? session.receiver : session.sharer;
  }
  if (!peer) return;  // counterpart hasn't attached yet — drop silently

  if (ws.role === 'sharer') session.bytesSharer += buf.length;
  else session.bytesReceiver += buf.length;

  // Enforce data limit (counts receiver→sharer traffic, which is what the
  // receiver is actually consuming).
  const limit = session.limits.dataLimitBytes;
  if (limit > 0 && session.bytesReceiver >= limit) {
    endSession(ws.sessionId, 'data_limit_reached');
    return;
  }

  // Enforce duration limit.
  const dur = session.limits.durationMs;
  if (dur > 0 && Date.now() - session.startedAt >= dur) {
    endSession(ws.sessionId, 'duration_reached');
    return;
  }

  sendBinary(peer, buf);
}

// Heartbeat: drop dead connections that didn't reply to our last ping.
setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) {
      try { ws.terminate(); } catch {}
      return;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  });
}, 30_000);

httpServer.listen(PORT, HOST, () => {
  console.log(`[relay] listening on ws://${HOST}:${PORT}`);
  console.log('[relay] health endpoint: GET /health');
});

/**
 * Reliability test for the relay's data-plane reconnect path.
 *
 * Complements smoke-test.js (which covers the CONTROL socket grace + resume).
 * Here we focus on the NATIVE data channels — the second socket each device
 * opens and binds to its session via `native_attach`. The native RelaySocket
 * now rebinds + reconnects on every transport switch, so the server must let a
 * data channel drop and reattach WITHOUT tearing the session down.
 *
 * Flow:
 *   1. Establish a full session (sharer + receiver control sockets).
 *   2. Both open native data channels via `native_attach`.
 *   3. Binary frame receiver→sharer is forwarded.
 *   4. Receiver's data channel drops (simulated transport switch).
 *      → sharer's data channel is told peer_native_disconnected
 *      → session stays alive (control sockets untouched).
 *   5. Receiver opens a FRESH data channel and re-attaches.
 *   6. Binary frames flow again in BOTH directions.
 *
 * Run: `node reliability-test.js`. Exits 0 on success, 1 on any failure.
 */

const { spawn } = require('child_process');
const WebSocket = require('ws');

const url = 'ws://127.0.0.1:9912';
process.env.PORT = 9912;
process.env.CONTROL_REATTACH_GRACE_MS = 1000;

const server = spawn(process.execPath, ['server.js'], {
  cwd: __dirname,
  env: process.env,
  stdio: ['ignore', 'pipe', 'pipe'],
});
server.stdout.on('data', d => process.stdout.write(`[srv] ${d}`));
server.stderr.on('data', d => process.stderr.write(`[srv ERR] ${d}`));

function expect(cond, msg) {
  if (!cond) {
    console.error(`✗ ${msg}`);
    server.kill();
    process.exit(1);
  }
  console.log(`✓ ${msg}`);
}

function nextMessage(ws, predicate, timeout = 5000) {
  return new Promise((resolve, reject) => {
    const onMsg = (raw, isBinary) => {
      if (isBinary) return;
      let parsed;
      try { parsed = JSON.parse(raw.toString()); } catch { return; }
      if (predicate(parsed)) {
        ws.off('message', onMsg);
        resolve(parsed);
      }
    };
    ws.on('message', onMsg);
    setTimeout(() => {
      ws.off('message', onMsg);
      reject(new Error('timeout waiting for control predicate'));
    }, timeout);
  });
}

function nextBinary(ws, timeout = 5000) {
  return new Promise((resolve, reject) => {
    const onMsg = (data, isBinary) => {
      if (!isBinary) return;
      ws.off('message', onMsg);
      resolve(data);
    };
    ws.on('message', onMsg);
    setTimeout(() => {
      ws.off('message', onMsg);
      reject(new Error('timeout waiting for binary frame'));
    }, timeout);
  });
}

// Assert NO control message matching predicate arrives within `window` ms.
function expectNoMessage(ws, predicate, window = 1500) {
  return new Promise(resolve => {
    let seen = false;
    const onMsg = (raw, isBinary) => {
      if (isBinary) return;
      let parsed;
      try { parsed = JSON.parse(raw.toString()); } catch { return; }
      if (predicate(parsed)) seen = true;
    };
    ws.on('message', onMsg);
    setTimeout(() => {
      ws.off('message', onMsg);
      resolve(!seen);
    }, window);
  });
}

function openSocket() {
  const ws = new WebSocket(url);
  return new Promise(r => ws.once('open', () => r(ws)));
}

async function attachNative(deviceId, role) {
  const ws = await openSocket();
  ws.send(JSON.stringify({ type: 'native_attach', deviceId, role }));
  await nextMessage(ws, m => m.type === 'native_attach_ack');
  return ws;
}

async function main() {
  await new Promise(r => setTimeout(r, 500));

  // ── 1. Establish a session via the control sockets. ────────────────────
  const sharer = await openSocket();
  const receiver = await openSocket();
  sharer.send(JSON.stringify({ type: 'hello', deviceId: 'shr' }));
  receiver.send(JSON.stringify({ type: 'hello', deviceId: 'rcv' }));
  const sHello = await nextMessage(sharer, m => m.type === 'hello_ack');
  await nextMessage(receiver, m => m.type === 'hello_ack');

  receiver.send(JSON.stringify({ type: 'connect_request', code: sHello.code }));
  const incoming = await nextMessage(sharer, m => m.type === 'incoming_request');
  sharer.send(JSON.stringify({
    type: 'approve_request',
    requestId: incoming.requestId,
    durationMs: 0,
    dataLimitBytes: 0,
  }));
  await nextMessage(sharer, m => m.type === 'session_started');
  await nextMessage(receiver, m => m.type === 'session_started');
  expect(true, 'session established between sharer and receiver');

  // ── 2. Both sides open native data channels. ───────────────────────────
  const sharerData = await attachNative('shr', 'sharer');
  let receiverData = await attachNative('rcv', 'receiver');
  expect(true, 'both native data channels attached to the session');

  // ── 3. Binary receiver→sharer forwarded. ───────────────────────────────
  {
    const payload = Buffer.from('frame-before-blip');
    const got = nextBinary(sharerData);
    receiverData.send(payload, { binary: true });
    expect(Buffer.compare(payload, await got) === 0, 'data frame forwarded before the blip');
  }

  // ── 4. Receiver data channel drops (transport switch). ─────────────────
  const sawNativeDown = nextMessage(sharerData, m => m.type === 'peer_native_disconnected');
  // The control sockets must NOT be told the session ended.
  const sessionSurvives = expectNoMessage(
    receiver,
    m => m.type === 'session_ended' || m.type === 'peer_disconnected',
  );
  receiverData.close();
  await sawNativeDown;
  expect(true, 'sharer data channel notified its peer native channel dropped');
  expect(await sessionSurvives, 'control session survived a native data-channel drop');

  // ── 5. Receiver re-attaches a fresh data channel. ──────────────────────
  receiverData = await attachNative('rcv', 'receiver');
  expect(true, 'receiver re-attached a fresh native data channel');

  // ── 6. Frames flow again, both directions. ─────────────────────────────
  {
    const payload = Buffer.from('frame-after-reconnect');
    const got = nextBinary(sharerData);
    receiverData.send(payload, { binary: true });
    expect(Buffer.compare(payload, await got) === 0, 'receiver→sharer frame forwarded after reconnect');
  }
  {
    const payload = Buffer.from('reply-after-reconnect');
    const got = nextBinary(receiverData);
    sharerData.send(payload, { binary: true });
    expect(Buffer.compare(payload, await got) === 0, 'sharer→receiver frame forwarded after reconnect');
  }

  sharer.close();
  receiver.close();
  sharerData.close();
  receiverData.close();
  server.kill();
  console.log('\n✅ all reliability tests passed');
  process.exit(0);
}

main().catch(err => {
  console.error('test failed:', err);
  server.kill();
  process.exit(1);
});

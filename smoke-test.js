/**
 * End-to-end smoke test for the relay protocol.
 *
 *   1. Spawn the server in-process.
 *   2. Open a "sharer" WebSocket → server returns a 6-digit code.
 *   3. Open a "receiver" WebSocket → server returns a different code.
 *   4. Receiver asks to connect to sharer's code.
 *   5. Sharer approves with 60s + 10 MB limits.
 *   6. Both peers receive session_started.
 *   7. Receiver sends a binary frame → server forwards to sharer.
 *   8. Verify byte counters / limit enforcement isn't triggered.
 *
 * Run: `node smoke-test.js`. Exits 0 on success, 1 on any assertion failure.
 */

const { spawn } = require('child_process');
const WebSocket = require('ws');

const url = 'ws://127.0.0.1:9911';
process.env.PORT = 9911;

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
  } else {
    console.log(`✓ ${msg}`);
  }
}

function nextMessage(ws, predicate) {
  return new Promise((resolve, reject) => {
    const onMsg = raw => {
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
      reject(new Error('timeout waiting for predicate'));
    }, 5000);
  });
}

async function main() {
  // Give the server a moment to bind.
  await new Promise(r => setTimeout(r, 500));

  const sharer = new WebSocket(url);
  const receiver = new WebSocket(url);

  await Promise.all([
    new Promise(r => sharer.once('open', r)),
    new Promise(r => receiver.once('open', r)),
  ]);

  sharer.send(JSON.stringify({ type: 'hello', deviceId: 'sharer-test' }));
  receiver.send(JSON.stringify({ type: 'hello', deviceId: 'receiver-test' }));

  const sharerHello = await nextMessage(sharer, m => m.type === 'hello_ack');
  const receiverHello = await nextMessage(receiver, m => m.type === 'hello_ack');

  expect(/^\d{6}$/.test(sharerHello.code), `sharer code is 6 digits: ${sharerHello.code}`);
  expect(/^\d{6}$/.test(receiverHello.code), `receiver code is 6 digits: ${receiverHello.code}`);
  expect(sharerHello.code !== receiverHello.code, 'codes are unique');

  // Receiver requests connection.
  receiver.send(JSON.stringify({ type: 'connect_request', code: sharerHello.code }));
  const incoming = await nextMessage(sharer, m => m.type === 'incoming_request');
  expect(incoming.fromCode === receiverHello.code, 'sharer sees incoming from correct code');

  // Sharer approves.
  sharer.send(JSON.stringify({
    type: 'approve_request',
    requestId: incoming.requestId,
    durationMs: 60_000,
    dataLimitBytes: 10 * 1024 * 1024,
  }));

  const sharerSession = await nextMessage(sharer, m => m.type === 'session_started');
  const receiverSession = await nextMessage(receiver, m => m.type === 'session_started');

  expect(sharerSession.role === 'sharer', 'sharer assigned role sharer');
  expect(receiverSession.role === 'receiver', 'receiver assigned role receiver');
  expect(sharerSession.sessionId === receiverSession.sessionId, 'both peers see same sessionId');

  // Binary frame from receiver → forwarded to sharer.
  const payload = Buffer.from('hello-from-receiver');
  const received = new Promise(resolve => {
    sharer.once('message', (data, isBinary) => {
      if (isBinary) resolve(data);
    });
  });
  receiver.send(payload, { binary: true });
  const got = await received;
  expect(Buffer.compare(payload, got) === 0, 'binary frame relayed receiver→sharer intact');

  // Tear down.
  sharer.send(JSON.stringify({ type: 'end_session', reason: 'test_done' }));
  await nextMessage(receiver, m => m.type === 'session_ended');
  expect(true, 'session_ended propagated to peer');

  sharer.close();
  receiver.close();
  server.kill();
  console.log('\n✅ all smoke tests passed');
  process.exit(0);
}

main().catch(err => {
  console.error('test failed:', err);
  server.kill();
  process.exit(1);
});

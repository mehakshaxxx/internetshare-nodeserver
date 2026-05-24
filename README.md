# InternetShare Relay Server

Tiny Node.js WebSocket relay. Pairs sharer/receiver phones by 6-digit code and forwards binary frames between them.

## Run locally

```bash
cd server
npm install
PORT=8080 npm start
```

Health check: `GET http://localhost:8080/health`

## Deploy on cPanel (Node.js app)

1. cPanel → **Setup Node.js App** → Create Application.
   - Node version: 18+
   - Application mode: **Production**
   - Application root: `relay` (or whatever folder you uploaded to)
   - Application URL: a subdomain like `relay.yourdomain.com`
   - Application startup file: `server.js`
2. Upload `package.json` and `server.js` to that folder via cPanel File Manager (or git clone).
3. In the Node.js app page, click **Run NPM Install**.
4. Set environment variable `PORT` to whatever cPanel binds (often `cPanel` sets this automatically). If using Passenger, just leave `PORT` blank — Passenger handles it.
5. Click **Start App**.

In your Apache config (cPanel handles this if you set the Application URL), make sure WebSocket upgrade is allowed:

```
RewriteEngine On
RewriteCond %{HTTP:Upgrade} websocket [NC]
RewriteCond %{HTTP:Connection} upgrade [NC]
RewriteRule ^/?(.*) "ws://127.0.0.1:PASSENGER_PORT/$1" [P,L]
```

(Replace `PASSENGER_PORT` with whatever cPanel assigned.)

## Deploy on bare VPS

```bash
# SSH into VPS
git clone <your-repo> && cd server
npm install
# Run as service:
sudo npm install -g pm2
pm2 start server.js --name internetshare-relay
pm2 save
pm2 startup
```

Put nginx in front for TLS:

```nginx
server {
  listen 443 ssl;
  server_name relay.yourdomain.com;
  ssl_certificate     /etc/letsencrypt/live/relay.yourdomain.com/fullchain.pem;
  ssl_certificate_key /etc/letsencrypt/live/relay.yourdomain.com/privkey.pem;

  location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_read_timeout 600s;
  }
}
```

After deploy, the app talks to `wss://relay.yourdomain.com`.

## Wire protocol

Control frames (text JSON):

| from → to        | type                | payload                                          |
|------------------|---------------------|--------------------------------------------------|
| client → server  | `hello`             | `{deviceId}`                                     |
| server → client  | `hello_ack`         | `{code, serverTime}`                             |
| receiver → srv   | `connect_request`   | `{code}`                                         |
| server → sharer  | `incoming_request`  | `{requestId, fromCode}`                          |
| sharer → server  | `approve_request`   | `{requestId, durationMs, dataLimitBytes}`        |
| sharer → server  | `reject_request`    | `{requestId}`                                    |
| server → both    | `session_started`   | `{sessionId, role, startedAt, limits}`           |
| both → server    | `end_session`       | `{reason?}`                                      |
| server → both    | `session_ended`     | `{reason}`                                       |
| server → both    | `peer_disconnected` | `{}`                                             |
| both → server    | `stats_update`      | `{stats: {...}}`                                 |
| client → server  | `ping`              | `{}`                                             |

Data frames (binary): once `session_started` is received, either side can send
raw binary frames and the server forwards them to the peer. The server only
counts bytes (for data limit) — it never inspects them.

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const {
  MSG_TYPE, encode, decode,
  makeNodes, makeNodeJoined, makeNodeLeft
} = require('../shared/protocol');

const PORT = parseInt(process.env.SIGNAL_PORT || '9000', 10);
const FRONTEND_DIR = path.join(__dirname, '..', 'frontend');

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.ico': 'image/x-icon'
};

const nodes = new Map();
let nextConnId = 0;

const log = (...args) => console.log(`[SignalServer]`, ...args);

function serveStatic(req, res) {
  let urlPath = req.url === '/' ? '/index.html' : req.url;
  const filePath = path.join(FRONTEND_DIR, urlPath);

  if (!filePath.startsWith(FRONTEND_DIR)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not Found');
      return;
    }
    const ext = path.extname(filePath);
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';
    res.writeHead(200, { 'Content-Type': contentType });
    res.end(data);
  });
}

function broadcast(msg, excludeId) {
  const payload = encode(msg);
  for (const [id, node] of nodes) {
    if (id !== excludeId && node.ws.readyState === 1) {
      node.ws.send(payload);
    }
  }
}

function sendTo(ws, msg) {
  if (ws.readyState === 1) {
    ws.send(encode(msg));
  }
}

function handleConnection(ws) {
  const connId = `conn-${++nextConnId}`;
  let registeredId = null;

  log(`new connection: ${connId}`);

  ws.on('message', (raw) => {
    const msg = decode(raw);
    if (!msg) return;

    switch (msg.type) {
      case MSG_TYPE.REGISTER: {
        registeredId = msg.nodeId;
        const info = msg.info || {};

        if (nodes.has(registeredId)) {
          const old = nodes.get(registeredId);
          old.ws.close(4001, 'replaced by new connection');
        }

        nodes.set(registeredId, { ws, info, connId });
        log(`node registered: ${registeredId} (${info.role || 'unknown'})`);

        sendTo(ws, makeNodes(
          Array.from(nodes.entries())
            .filter(([id]) => id !== registeredId)
            .map(([id, n]) => ({ id, ...n.info }))
        ));

        broadcast(makeNodeJoined(registeredId, info), registeredId);
        break;
      }

      case MSG_TYPE.SIGNAL: {
        const target = nodes.get(msg.targetId);
        if (target) {
          sendTo(target.ws, {
            type: MSG_TYPE.SIGNAL,
            fromId: registeredId,
            signal: msg.signal
          });
        }
        break;
      }

      case MSG_TYPE.RELAY: {
        const target = nodes.get(msg.targetId);
        if (target) {
          sendTo(target.ws, {
            type: MSG_TYPE.RELAY,
            fromId: registeredId,
            data: msg.data
          });
        }
        break;
      }

      default:
        log(`unknown message type from ${registeredId}: ${msg.type}`);
    }
  });

  ws.on('close', (code, reason) => {
    log(`connection closed: ${connId} (code=${code})`);
    if (registeredId && nodes.has(registeredId)) {
      const node = nodes.get(registeredId);
      if (node.connId === connId) {
        nodes.delete(registeredId);
        broadcast(makeNodeLeft(registeredId));
        log(`node unregistered: ${registeredId}`);
      }
    }
  });

  ws.on('error', (err) => {
    log(`ws error for ${connId}:`, err.message);
  });
}

function main() {
  const server = http.createServer(serveStatic);

  const wss = new WebSocketServer({ server });

  wss.on('connection', handleConnection);

  server.listen(PORT, () => {
    log(`signaling server + frontend running at http://localhost:${PORT}`);
    log(`WebSocket endpoint: ws://localhost:${PORT}`);
    log(`agents connect via WebSocket for signaling + relay`);
  });

  process.on('SIGINT', () => {
    log('shutting down...');
    for (const [, node] of nodes) {
      node.ws.close(1001, 'server shutdown');
    }
    server.close(() => process.exit(0));
  });
}

main();

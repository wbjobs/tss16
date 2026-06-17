const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const os = require('os');
const {
  MSG_TYPE, LOG_LEVELS, LOG_LEVEL_ORDER,
  encode, decode,
  makeRegister, makeSignal, makeRelay,
  makeQuery, makeQueryCancel,
  makeLogStream, makeLogStreamEnd,
  makePing, makePong
} = require('../shared/protocol');

let wrtc = null;
try {
  wrtc = require('wrtc');
  console.log('[Agent] wrtc loaded — P2P mode available');
} catch {
  console.log('[Agent] wrtc not available — relay-only mode');
}

const RTC_CONFIG = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' }
  ]
};

const SIGNAL_URL = process.env.SIGNAL_URL || 'ws://localhost:9000';
const NODE_ID = process.env.NODE_ID || `agent-${os.hostname()}-${process.pid}`;
const LOG_DIR = process.env.LOG_DIR || '';
const SIMULATE = process.argv.includes('--simulate');

const log = (...args) => console.log(`[${NODE_ID}]`, ...args);

class PeerConnection {
  constructor(nodeId, agent) {
    this.nodeId = nodeId;
    this.agent = agent;
    this.pc = null;
    this.dc = null;
    this.isInitiator = false;
    this.connected = false;
    this.relayOnly = false;
  }

  async initiate() {
    if (!wrtc) {
      this.relayOnly = true;
      log(`peer ${this.nodeId}: relay-only mode`);
      this.connected = true;
      return;
    }

    this.isInitiator = true;
    this._createPC();

    this.dc = this.pc.createDataChannel('logGrid', {
      ordered: true,
      maxRetransmits: 3
    });
    this._setupDC(this.dc);

    try {
      const offer = await this.pc.createOffer();
      await this.pc.setLocalDescription(offer);
      this.agent.sendSignal(this.nodeId, { type: 'offer', sdp: this.pc.localDescription });
    } catch (err) {
      log(`peer ${this.nodeId}: create offer failed:`, err.message);
      this._fallbackToRelay();
    }
  }

  async handleSignal(signal) {
    if (!wrtc) {
      this.relayOnly = true;
      this.connected = true;
      this.agent.sendSignal(this.nodeId, { type: 'relay-fallback' });
      return;
    }

    try {
      if (signal.type === 'offer') {
        if (!this.pc) {
          this._createPC();
        }
        await this.pc.setRemoteDescription(new wrtc.RTCSessionDescription(signal.sdp));
        const answer = await this.pc.createAnswer();
        await this.pc.setLocalDescription(answer);
        this.agent.sendSignal(this.nodeId, { type: 'answer', sdp: this.pc.localDescription });
      } else if (signal.type === 'answer') {
        if (!this.pc) return;
        await this.pc.setRemoteDescription(new wrtc.RTCSessionDescription(signal.sdp));
      } else if (signal.type === 'candidate') {
        if (!this.pc) return;
        await this.pc.addIceCandidate(new wrtc.RTCIceCandidate(signal.candidate));
      }
    } catch (err) {
      log(`peer ${this.nodeId}: signal handling failed:`, err.message);
      this._fallbackToRelay();
    }
  }

  _createPC() {
    this.pc = new wrtc.RTCPeerConnection(RTC_CONFIG);

    this.pc.onicecandidate = (evt) => {
      if (evt.candidate) {
        this.agent.sendSignal(this.nodeId, {
          type: 'candidate',
          candidate: evt.candidate
        });
      }
    };

    this.pc.ondatachannel = (evt) => {
      this.dc = evt.channel;
      this._setupDC(this.dc);
    };

    this.pc.onconnectionstatechange = () => {
      const state = this.pc.connectionState;
      log(`peer ${this.nodeId}: connection state = ${state}`);
      if (state === 'connected') {
        this.connected = true;
        this.relayOnly = false;
      } else if (state === 'failed' || state === 'disconnected') {
        this._fallbackToRelay();
      }
    };

    this.pc.oniceconnectionstatechange = () => {
      const state = this.pc.iceConnectionState;
      log(`peer ${this.nodeId}: ICE state = ${state}`);
      if (state === 'failed') {
        this._fallbackToRelay();
      }
    };
  }

  _setupDC(dc) {
    dc.onopen = () => {
      log(`peer ${this.nodeId}: data channel open`);
      this.connected = true;
      this.relayOnly = false;
    };
    dc.onclose = () => {
      log(`peer ${this.nodeId}: data channel closed`);
      this._fallbackToRelay();
    };
    dc.onmessage = (evt) => {
      const msg = decode(evt.data);
      if (msg) this.agent.handleMeshMessage(this.nodeId, msg);
    };
  }

  _fallbackToRelay() {
    if (this.pc) {
      try { this.pc.close(); } catch {}
      this.pc = null;
      this.dc = null;
    }
    this.relayOnly = true;
    this.connected = true;
    log(`peer ${this.nodeId}: fell back to relay mode`);
  }

  send(msg) {
    const payload = encode(msg);
    if (this.dc && this.dc.readyState === 'open') {
      this.dc.send(payload);
    } else {
      this.agent.sendRelay(this.nodeId, msg);
    }
  }

  close() {
    if (this.dc) {
      try { this.dc.close(); } catch {}
    }
    if (this.pc) {
      try { this.pc.close(); } catch {}
    }
    this.connected = false;
  }
}

class LogTailer {
  constructor(agent) {
    this.agent = agent;
    this.watchers = [];
    this.positions = new Map();
    this.simInterval = null;
  }

  start() {
    if (SIMULATE) {
      this._startSimulation();
      return;
    }
    if (!LOG_DIR) {
      log('no LOG_DIR set and --simulate not used, generating simulated logs');
      this._startSimulation();
      return;
    }

    const dir = path.resolve(LOG_DIR);
    if (!fs.existsSync(dir)) {
      log(`LOG_DIR not found: ${dir}, falling back to simulation`);
      this._startSimulation();
      return;
    }

    this._watchDir(dir);
    log(`watching log directory: ${dir}`);
  }

  _watchDir(dir) {
    const initialFiles = fs.readdirSync(dir).filter(f => f.endsWith('.log'));
    for (const file of initialFiles) {
      this._tailFile(path.join(dir, file));
    }

    fs.watch(dir, (eventType, filename) => {
      if (filename && filename.endsWith('.log')) {
        const filePath = path.join(dir, filename);
        if (eventType === 'rename' && fs.existsSync(filePath) && !this.positions.has(filePath)) {
          this._tailFile(filePath);
        }
      }
    });
  }

  _tailFile(filePath) {
    const fileName = path.basename(filePath);
    const stat = fs.statSync(filePath);
    this.positions.set(filePath, stat.size);

    const watch = fs.watch(filePath, (eventType) => {
      if (eventType !== 'change') return;
      try {
        const currentSize = fs.statSync(filePath).size;
        const prevPos = this.positions.get(filePath) || 0;

        if (currentSize < prevPos) {
          this.positions.set(filePath, 0);
          return;
        }

        if (currentSize > prevPos) {
          const stream = fs.createReadStream(filePath, {
            start: prevPos,
            end: currentSize - 1,
            encoding: 'utf8'
          });
          let data = '';
          stream.on('data', (chunk) => { data += chunk; });
          stream.on('end', () => {
            this.positions.set(filePath, currentSize);
            const lines = data.split('\n').filter(l => l.trim());
            for (const line of lines) {
              this.agent.onLocalLog(fileName, line);
            }
          });
        }
      } catch {}
    });

    this.watchers.push(watch);
  }

  _startSimulation() {
    log('starting log simulation mode');
    const services = ['api-gateway', 'auth-service', 'order-service', 'payment-service', 'user-service'];
    const messages = [
      'Request processed successfully in {ms}ms',
      'Connection established from {ip}',
      'Cache hit ratio: {pct}%',
      'Database query executed in {ms}ms',
      'Health check passed',
      'Rate limit threshold reached for {ip}',
      'Session expired for user-{uid}',
      'Worker thread pool utilization: {pct}%',
      'Outbound request to upstream timed out after {ms}ms',
      'Configuration reloaded from disk',
      'JWT token validated for user-{uid}',
      'Message published to queue {queue}',
      'GC pause detected: {ms}ms',
      'File descriptor limit at {pct}%',
      'TLS handshake completed with {ip}'
    ];

    const generate = () => {
      const level = LOG_LEVELS[Math.random() < 0.5 ? 0 : Math.random() < 0.7 ? 1 : Math.random() < 0.9 ? 2 : 3];
      const service = services[Math.floor(Math.random() * services.length)];
      let message = messages[Math.floor(Math.random() * messages.length)];
      message = message
        .replace('{ms}', Math.floor(Math.random() * 2000))
        .replace('{pct}', Math.floor(Math.random() * 100))
        .replace('{ip}', `10.${Math.floor(Math.random()*255)}.${Math.floor(Math.random()*255)}.${Math.floor(Math.random()*255)}`)
        .replace('{uid}', Math.floor(Math.random() * 10000))
        .replace('{queue}', ['email', 'push', 'webhook', 'retry'][Math.floor(Math.random() * 4)]);

      const now = new Date();
      const timestamp = now.toISOString().replace('T', ' ').replace('Z', '');
      const line = `${timestamp} [${level}] [${service}] ${message}`;

      this.agent.onLocalLog(service, line);
    };

    const scheduleNext = () => {
      const delay = 200 + Math.random() * 2000;
      this.simInterval = setTimeout(() => {
        generate();
        if (Math.random() < 0.2) {
          generate();
          generate();
        }
        scheduleNext();
      }, delay);
    };
    scheduleNext();
  }

  stop() {
    for (const w of this.watchers) {
      try { w.close(); } catch {}
    }
    if (this.simInterval) {
      clearTimeout(this.simInterval);
    }
  }
}

class Agent {
  constructor() {
    this.ws = null;
    this.peers = new Map();
    this.tailer = new LogTailer(this);
    this.localLogs = [];
    this.maxLocalLogs = 5000;
    this.activeQueries = new Map();
    this.seenQueryIds = new Set();
    this.maxSeenQueries = 1000;
    this.reconnectTimer = null;
  }

  async start() {
    log(`starting agent (id=${NODE_ID})`);
    log(`signal server: ${SIGNAL_URL}`);
    log(`simulate: ${SIMULATE}`);

    this._connectSignal();
    this.tailer.start();

    this._pingInterval = setInterval(() => {
      for (const [peerId, peer] of this.peers) {
        if (peer.connected) {
          peer.send(makePing());
        }
      }
    }, 30000);
  }

  _connectSignal() {
    log(`connecting to signaling server...`);
    this.ws = new WebSocket(SIGNAL_URL);

    this.ws.on('open', () => {
      log('connected to signaling server');
      this.ws.send(encode(makeRegister(NODE_ID, {
        role: 'agent',
        hostname: os.hostname(),
        platform: os.platform(),
        uptime: os.uptime(),
        simulate: SIMULATE
      })));
    });

    this.ws.on('message', (raw) => {
      const msg = decode(raw);
      if (!msg) return;
      this._handleSignalMessage(msg);
    });

    this.ws.on('close', (code) => {
      log(`disconnected from signaling server (code=${code})`);
      this._scheduleReconnect();
    });

    this.ws.on('error', (err) => {
      log(`signaling connection error: ${err.message}`);
      this._scheduleReconnect();
    });
  }

  _scheduleReconnect() {
    if (this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this._connectSignal();
    }, 3000);
  }

  _handleSignalMessage(msg) {
    switch (msg.type) {
      case MSG_TYPE.NODES: {
        for (const node of msg.nodes) {
          this._connectToPeer(node.id);
        }
        break;
      }
      case MSG_TYPE.NODE_JOINED: {
        this._connectToPeer(msg.nodeId);
        break;
      }
      case MSG_TYPE.NODE_LEFT: {
        this._removePeer(msg.nodeId);
        break;
      }
      case MSG_TYPE.SIGNAL: {
        this._handleIncomingSignal(msg.fromId, msg.signal);
        break;
      }
      case MSG_TYPE.RELAY: {
        const inner = msg.data;
        if (inner) this.handleMeshMessage(msg.fromId, inner);
        break;
      }
    }
  }

  _connectToPeer(peerId) {
    if (peerId === NODE_ID) return;
    if (this.peers.has(peerId)) return;

    const peer = new PeerConnection(peerId, this);
    this.peers.set(peerId, peer);
    peer.initiate();
    log(`initiating connection to peer: ${peerId}`);
  }

  _removePeer(peerId) {
    const peer = this.peers.get(peerId);
    if (peer) {
      peer.close();
      this.peers.delete(peerId);
      log(`peer removed: ${peerId}`);
    }
  }

  _handleIncomingSignal(fromId, signal) {
    let peer = this.peers.get(fromId);
    if (!peer) {
      peer = new PeerConnection(fromId, this);
      this.peers.set(fromId, peer);
    }
    peer.handleSignal(signal);
  }

  sendSignal(targetId, signal) {
    if (this.ws && this.ws.readyState === 1) {
      this.ws.send(encode(makeSignal(targetId, signal)));
    }
  }

  sendRelay(targetId, msg) {
    if (this.ws && this.ws.readyState === 1) {
      this.ws.send(encode(makeRelay(targetId, msg)));
    }
  }

  onLocalLog(source, line) {
    const levelMatch = line.match(/\[(DEBUG|INFO|WARN|ERROR)\]/);
    const level = levelMatch ? levelMatch[1] : 'INFO';

    const entry = {
      timestamp: Date.now(),
      source,
      level,
      message: line,
      nodeId: NODE_ID,
      hostname: os.hostname()
    };

    this.localLogs.push(entry);
    if (this.localLogs.length > this.maxLocalLogs) {
      this.localLogs = this.localLogs.slice(-this.maxLocalLogs);
    }

    for (const [queryId, query] of this.activeQueries) {
      if (this._matchesFilter(entry, query.filters)) {
        this._sendToPeer(query.sourceId, makeLogStream(queryId, NODE_ID, entry));
      }
    }
  }

  handleMeshMessage(fromId, msg) {
    if (!msg || !msg.type) return;

    switch (msg.type) {
      case MSG_TYPE.QUERY:
        this._handleQuery(fromId, msg);
        break;
      case MSG_TYPE.QUERY_CANCEL:
        this._handleQueryCancel(msg);
        break;
      case MSG_TYPE.LOG_STREAM:
      case MSG_TYPE.LOG_STREAM_END:
        this._forwardLogMessage(fromId, msg);
        break;
      case MSG_TYPE.PING:
        this._sendToPeer(fromId, makePong(msg.ts));
        break;
      case MSG_TYPE.PONG:
        break;
    }
  }

  _handleQuery(fromId, msg) {
    if (this.seenQueryIds.has(msg.queryId)) return;
    this.seenQueryIds.add(msg.queryId);

    if (this.seenQueryIds.size > this.maxSeenQueries) {
      const iter = this.seenQueryIds.values();
      this.seenQueryIds.delete(iter.next().value);
    }

    this.activeQueries.set(msg.queryId, { sourceId: msg.sourceId, filters: msg.filters, via: fromId });

    const matching = this.localLogs.filter(e => this._matchesFilter(e, msg.filters));
    for (const entry of matching) {
      this._sendToPeer(fromId, makeLogStream(msg.queryId, NODE_ID, entry));
    }
    this._sendToPeer(fromId, makeLogStreamEnd(msg.queryId, NODE_ID));

    if (msg.ttl > 0) {
      const forwardMsg = makeQuery(msg.queryId, msg.sourceId, msg.filters, msg.ttl - 1);
      for (const [peerId, peer] of this.peers) {
        if (peerId !== fromId && peer.connected) {
          peer.send(forwardMsg);
        }
      }
    }
  }

  _handleQueryCancel(msg) {
    this.activeQueries.delete(msg.queryId);
    for (const [peerId, peer] of this.peers) {
      if (peerId !== msg.sourceId && peer.connected) {
        peer.send(msg);
      }
    }
  }

  _forwardLogMessage(fromId, msg) {
    const query = this.activeQueries.get(msg.queryId);
    if (!query) return;
    this._sendToPeer(query.via, msg);
  }

  _sendToPeer(peerId, msg) {
    const peer = this.peers.get(peerId);
    if (peer && peer.connected) {
      peer.send(msg);
    } else {
      this.sendRelay(peerId, msg);
    }
  }

  _matchesFilter(entry, filters) {
    if (!filters) return true;
    if (filters.minLevel) {
      const entryOrder = LOG_LEVEL_ORDER[entry.level] || 0;
      const minOrder = LOG_LEVEL_ORDER[filters.minLevel] || 0;
      if (entryOrder < minOrder) return false;
    }
    if (filters.keyword) {
      const kw = filters.keyword.toLowerCase();
      if (!entry.message.toLowerCase().includes(kw)) return false;
    }
    if (filters.source) {
      if (entry.source !== filters.source) return false;
    }
    return true;
  }

  stop() {
    this.tailer.stop();
    if (this._pingInterval) clearInterval(this._pingInterval);
    for (const [, peer] of this.peers) {
      peer.close();
    }
    if (this.ws) this.ws.close();
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
  }
}

const agent = new Agent();
agent.start().catch(err => {
  console.error('agent failed to start:', err);
  process.exit(1);
});

process.on('SIGINT', () => {
  log('shutting down...');
  agent.stop();
  process.exit(0);
});

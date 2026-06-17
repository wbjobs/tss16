const MSG_TYPE = {
  REGISTER: 'register',
  NODES: 'nodes',
  NODE_JOINED: 'node-joined',
  NODE_LEFT: 'node-left',
  SIGNAL: 'signal',
  RELAY: 'relay',
  QUERY: 'query',
  QUERY_CANCEL: 'query-cancel',
  LOG_STREAM: 'log-stream',
  LOG_STREAM_END: 'log-stream-end',
  PING: 'ping',
  PONG: 'pong',
  COLLECT_REQ: 'collect-req',
  COLLECT_ACK: 'collect-ack',
  COLLECT_DATA: 'collect-data',
  COLLECT_END: 'collect-end',
  HEALTH_STATS: 'health-stats'
};

const LOG_LEVEL_ORDER = { DEBUG: 0, INFO: 1, WARN: 2, ERROR: 3 };

const RTC_CONFIG = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' }
  ]
};

const PING_INTERVAL_MS = 5000;
const PING_TIMEOUT_MS = 15000;
const RECONNECT_BASE_DELAY = 2000;
const RECONNECT_MAX_DELAY = 30000;

function makePing() {
  return { type: MSG_TYPE.PING, ts: Date.now(), t1: Date.now() };
}

function makePong(ts, t1) {
  return { type: MSG_TYPE.PONG, ts, t1: t1 || 0, t2: Date.now() };
}

class PeerConnection {
  constructor(nodeId, grid) {
    this.nodeId = nodeId;
    this.grid = grid;
    this.pc = null;
    this.dc = null;
    this.connected = false;
    this.relayOnly = false;
    this.lastPongAt = Date.now();
    this.lastPingAt = 0;
    this.clockOffset = 0;
    this.rtt = 0;
    this.reconnectAttempts = 0;
    this.reconnectTimer = null;
    this.dead = false;
  }

  async initiate() {
    this._createPC();

    this.dc = this.pc.createDataChannel('logGrid', {
      ordered: true,
      maxRetransmits: 3
    });
    this._setupDC(this.dc);

    try {
      const offer = await this.pc.createOffer();
      await this.pc.setLocalDescription(offer);
      this.grid.sendSignal(this.nodeId, { type: 'offer', sdp: this.pc.localDescription });
    } catch (err) {
      console.warn(`[Peer:${this.nodeId}] create offer failed:`, err);
      this._fallbackToRelay();
    }
  }

  async handleSignal(signal) {
    if (signal.type === 'relay-fallback') {
      this._fallbackToRelay();
      return;
    }

    try {
      if (signal.type === 'offer') {
        if (!this.pc) this._createPC();
        await this.pc.setRemoteDescription(new RTCSessionDescription(signal.sdp));
        const answer = await this.pc.createAnswer();
        await this.pc.setLocalDescription(answer);
        this.grid.sendSignal(this.nodeId, { type: 'answer', sdp: this.pc.localDescription });
      } else if (signal.type === 'answer') {
        if (!this.pc) return;
        await this.pc.setRemoteDescription(new RTCSessionDescription(signal.sdp));
      } else if (signal.type === 'candidate') {
        if (!this.pc) return;
        await this.pc.addIceCandidate(new RTCIceCandidate(signal.candidate));
      }
    } catch (err) {
      console.warn(`[Peer:${this.nodeId}] signal handling failed:`, err);
      this._fallbackToRelay();
    }
  }

  _createPC() {
    this.pc = new RTCPeerConnection(RTC_CONFIG);

    this.pc.onicecandidate = (evt) => {
      if (evt.candidate) {
        this.grid.sendSignal(this.nodeId, {
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
      console.log(`[Peer:${this.nodeId}] state=${state}`);
      if (state === 'connected') {
        this.connected = true;
        this.relayOnly = false;
        this.dead = false;
        this.lastPongAt = Date.now();
        this.reconnectAttempts = 0;
        this.grid.onPeerStateChange(this.nodeId);
      } else if (state === 'failed' || state === 'disconnected') {
        this._fallbackToRelay();
      }
    };

    this.pc.oniceconnectionstatechange = () => {
      if (this.pc.iceConnectionState === 'failed') {
        this._fallbackToRelay();
      }
    };
  }

  _setupDC(dc) {
    dc.binaryType = 'arraybuffer';
    dc.onopen = () => {
      console.log(`[Peer:${this.nodeId}] data channel open`);
      this.connected = true;
      this.relayOnly = false;
      this.dead = false;
      this.lastPongAt = Date.now();
      this.reconnectAttempts = 0;
      this.grid.onPeerStateChange(this.nodeId);
    };
    dc.onclose = () => {
      console.log(`[Peer:${this.nodeId}] data channel closed`);
      this._fallbackToRelay();
    };
    dc.onerror = () => {
      this._fallbackToRelay();
    };
    dc.onmessage = (evt) => {
      const text = typeof evt.data === 'string' ? evt.data : new TextDecoder().decode(evt.data);
      try {
        const msg = JSON.parse(text);
        this.grid.handleMeshMessage(this.nodeId, msg);
      } catch {}
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
    this.dead = false;
    this.lastPongAt = Date.now();
    this.reconnectAttempts = 0;
    console.log(`[Peer:${this.nodeId}] using relay mode`);
    this.grid.onPeerStateChange(this.nodeId);
  }

  markDead() {
    if (this.dead) return;
    this.dead = true;
    this.connected = false;
    console.warn(`[Peer:${this.nodeId}] MARKED DEAD (no pong for ${PING_TIMEOUT_MS}ms)`);
    this.grid.onPeerStateChange(this.nodeId);
    this._scheduleReconnect();
  }

  _scheduleReconnect() {
    if (this.reconnectTimer) return;
    this.reconnectAttempts++;
    const delay = Math.min(
      RECONNECT_BASE_DELAY * Math.pow(2, this.reconnectAttempts - 1),
      RECONNECT_MAX_DELAY
    );
    console.log(`[Peer:${this.nodeId}] scheduling reconnect in ${delay}ms (attempt ${this.reconnectAttempts})`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.pc = null;
      this.dc = null;
      if (this.grid.nodeInfo.has(this.nodeId)) {
        this.initiate();
      }
    }, delay);
  }

  send(msg) {
    const payload = JSON.stringify(msg);
    if (this.dc && this.dc.readyState === 'open') {
      try {
        this.dc.send(payload);
        return;
      } catch {}
    }
    this.grid.sendRelay(this.nodeId, msg);
  }

  close() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.dc) try { this.dc.close(); } catch {}
    if (this.pc) try { this.pc.close(); } catch {}
    this.connected = false;
    this.dead = true;
  }
}

class LogGrid {
  constructor() {
    this.ws = null;
    this.peers = new Map();
    this.nodeInfo = new Map();
    this.nodeId = `frontend-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    this.activeQueryId = null;
    this.logs = [];
    this.filteredLogs = [];
    this.maxLogs = 100000;
    this.stats = { total: 0, ERROR: 0, WARN: 0, INFO: 0, DEBUG: 0 };
    this.autoScroll = true;
    this.sources = new Set();
    this.reconnectTimer = null;
    this.wsReconnectAttempts = 0;
    this.signalUrl = '';
    this.sortDirty = false;

    this.healthData = new Map();
    this.activeCollects = new Map();
    this.charts = {};
    this.currentTab = 'logs';

    this.virtualScroll = null;
  }

  connect(signalUrl) {
    this.signalUrl = signalUrl;
    this.updateStatus('connecting', '连接中...');
    this.ws = new WebSocket(signalUrl);

    this.ws.onopen = () => {
      this.ws.send(JSON.stringify({
        type: MSG_TYPE.REGISTER,
        nodeId: this.nodeId,
        info: { role: 'frontend' }
      }));
      this.wsReconnectAttempts = 0;
      this.updateStatus('connected', '已连接');
    };

    this.ws.onmessage = (evt) => {
      const msg = JSON.parse(evt.data);
      this._handleSignalMessage(msg);
    };

    this.ws.onclose = () => {
      this.updateStatus('', '已断开');
      for (const [, peer] of this.peers) {
        if (!peer.relayOnly) {
          peer.markDead();
        }
      }
      this._scheduleSignalReconnect();
    };

    this.ws.onerror = () => {
      this.updateStatus('', '连接失败');
      this._scheduleSignalReconnect();
    };
  }

  _scheduleSignalReconnect() {
    if (this.reconnectTimer) return;
    if (!this.signalUrl) return;
    this.wsReconnectAttempts++;
    const delay = Math.min(
      RECONNECT_BASE_DELAY * Math.pow(2, this.wsReconnectAttempts - 1),
      RECONNECT_MAX_DELAY
    );
    this.updateStatus('connecting', `重连中 (${this.wsReconnectAttempts})...`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect(this.signalUrl);
    }, delay);
  }

  _handleSignalMessage(msg) {
    switch (msg.type) {
      case MSG_TYPE.NODES:
        for (const node of msg.nodes) {
          this.nodeInfo.set(node.id, node);
          this._connectToPeer(node.id);
        }
        this._updateNodeList();
        break;
      case MSG_TYPE.NODE_JOINED:
        this.nodeInfo.set(msg.nodeId, msg.info);
        this._connectToPeer(msg.nodeId);
        this._updateNodeList();
        break;
      case MSG_TYPE.NODE_LEFT:
        this._removePeer(msg.nodeId);
        this.nodeInfo.delete(msg.nodeId);
        this._updateNodeList();
        break;
      case MSG_TYPE.SIGNAL:
        this._handleIncomingSignal(msg.fromId, msg.signal);
        break;
      case MSG_TYPE.RELAY:
        if (msg.data) this.handleMeshMessage(msg.fromId, msg.data);
        break;
    }
  }

  _connectToPeer(peerId) {
    if (peerId === this.nodeId) return;
    if (this.peers.has(peerId)) {
      const existing = this.peers.get(peerId);
      if (existing.dead) {
        if (existing.reconnectTimer) {
          clearTimeout(existing.reconnectTimer);
          existing.reconnectTimer = null;
        }
        existing.initiate();
      }
      return;
    }

    const peer = new PeerConnection(peerId, this);
    this.peers.set(peerId, peer);
    peer.initiate();
  }

  _removePeer(peerId) {
    const peer = this.peers.get(peerId);
    if (peer) {
      peer.close();
      this.peers.delete(peerId);
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

  onPeerStateChange(nodeId) {
    this._updateNodeList();
  }

  sendSignal(targetId, signal) {
    if (this.ws && this.ws.readyState === 1) {
      this.ws.send(JSON.stringify({
        type: MSG_TYPE.SIGNAL,
        targetId,
        signal
      }));
    }
  }

  sendRelay(targetId, msg) {
    if (this.ws && this.ws.readyState === 1) {
      this.ws.send(JSON.stringify({
        type: MSG_TYPE.RELAY,
        targetId,
        data: msg
      }));
    }
  }

  startHeartbeat() {
    if (this._pingInterval) return;
    this._pingInterval = setInterval(() => {
      const now = Date.now();
      for (const [peerId, peer] of this.peers) {
        if (peer.connected && !peer.dead) {
          peer.lastPingAt = now;
          peer.send(makePing());
        }
        if (!peer.dead && now - peer.lastPongAt > PING_TIMEOUT_MS) {
          peer.markDead();
        }
      }
      if (this.sortDirty) {
        this._sortLogs();
        this.sortDirty = false;
      }
    }, PING_INTERVAL_MS);
  }

  stopHeartbeat() {
    if (this._pingInterval) {
      clearInterval(this._pingInterval);
      this._pingInterval = null;
    }
  }

  startQuery(filters) {
    this.stopQuery();

    const queryId = `q-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    this.activeQueryId = queryId;
    this.logs = [];
    this.filteredLogs = [];
    this.stats = { total: 0, ERROR: 0, WARN: 0, INFO: 0, DEBUG: 0 };
    this._updateStats();

    const query = {
      type: MSG_TYPE.QUERY,
      queryId,
      sourceId: this.nodeId,
      filters,
      ttl: 5
    };

    for (const [peerId, peer] of this.peers) {
      if (peer.connected) {
        peer.send(query);
      }
    }

    this.startHeartbeat();
    this.virtualScroll.setItems([]);
    return queryId;
  }

  stopQuery() {
    this.stopHeartbeat();
    if (!this.activeQueryId) return;

    const cancel = {
      type: MSG_TYPE.QUERY_CANCEL,
      queryId: this.activeQueryId,
      sourceId: this.nodeId
    };

    for (const [peerId, peer] of this.peers) {
      if (peer.connected) {
        peer.send(cancel);
      }
    }

    this.activeQueryId = null;
  }

  getClockOffset(nodeId) {
    const peer = this.peers.get(nodeId);
    return peer ? peer.clockOffset : 0;
  }

  handleMeshMessage(fromId, msg) {
    if (!msg || !msg.type) return;

    const peer = this.peers.get(fromId);
    if (peer && !peer.dead) {
      peer.lastPongAt = Date.now();
    }

    switch (msg.type) {
      case MSG_TYPE.LOG_STREAM:
        if (msg.queryId === this.activeQueryId) {
          this._addLog(msg.entry);
        }
        break;
      case MSG_TYPE.LOG_STREAM_END:
        break;
      case MSG_TYPE.PING: {
        if (peer) {
          peer.lastPongAt = Date.now();
        }
        const sendMsg = makePong(msg.ts, msg.t1);
        if (peer && peer.connected) {
          peer.send(sendMsg);
        } else {
          this.sendRelay(fromId, sendMsg);
        }
        break;
      }
      case MSG_TYPE.PONG: {
        if (peer) {
          const now = Date.now();
          const t1 = msg.t1 || peer.lastPingAt;
          const t2 = msg.t2 || msg.ts;
          const t3 = now;
          const rtt = t3 - t1;
          const offset = t2 - (t1 + t3) / 2;
          peer.rtt = rtt;
          peer.clockOffset = offset;
          peer.lastPongAt = now;
          peer.dead = false;
          peer.connected = true;
          this._updateNodeList();
          this.sortDirty = true;
        }
        break;
      }
      case MSG_TYPE.HEALTH_STATS: {
        this._handleHealthStats(fromId, msg);
        break;
      }
      case MSG_TYPE.COLLECT_ACK: {
        this._handleCollectAck(msg);
        break;
      }
      case MSG_TYPE.COLLECT_DATA: {
        this._handleCollectData(msg);
        break;
      }
      case MSG_TYPE.COLLECT_END: {
        this._handleCollectEnd(msg);
        break;
      }
    }
  }

  _sortLogs() {
    if (this.logs.length === 0) return;
    for (const entry of this.logs) {
      entry.calibratedTimestamp = entry.timestamp - this.getClockOffset(entry.nodeId);
    }
    this.logs.sort((a, b) => a.calibratedTimestamp - b.calibratedTimestamp);
    this.filteredLogs = this.logs.filter(e => this._matchesLocalFilter(e));
    this.virtualScroll.setItems(this.filteredLogs);
  }

  _addLog(entry) {
    if (this.logs.length >= this.maxLogs) {
      const removed = this.logs.splice(0, 1000);
      for (const r of removed) {
        this.stats[r.level] = Math.max(0, (this.stats[r.level] || 0) - 1);
      }
    }

    entry.calibratedTimestamp = entry.timestamp - this.getClockOffset(entry.nodeId);
    this.logs.push(entry);
    this.stats.total++;
    this.stats[entry.level] = (this.stats[entry.level] || 0) + 1;

    if (entry.source) {
      this.sources.add(entry.source);
      this._updateSourceFilter();
    }

    if (this._matchesLocalFilter(entry)) {
      this.filteredLogs.push(entry);
      this.virtualScroll.appendItem(entry);
    }

    this._updateStats();

    if (this.autoScroll) {
      this.virtualScroll.scrollToBottom();
    }
  }

  _matchesLocalFilter(entry) {
    const levelFilter = document.getElementById('levelFilter').value;
    const keywordFilter = document.getElementById('keywordFilter').value;

    if (levelFilter) {
      const entryOrder = LOG_LEVEL_ORDER[entry.level] || 0;
      const minOrder = LOG_LEVEL_ORDER[levelFilter] || 0;
      if (entryOrder < minOrder) return false;
    }

    if (keywordFilter) {
      if (!entry.message.toLowerCase().includes(keywordFilter.toLowerCase())) return false;
    }

    return true;
  }

  applyLocalFilter() {
    this.filteredLogs = this.logs.filter(e => this._matchesLocalFilter(e));
    this.virtualScroll.setItems(this.filteredLogs);
  }

  _updateNodeList() {
    const container = document.getElementById('nodeList');
    if (this.peers.size === 0) {
      container.innerHTML = '<div class="node-empty">暂无节点</div>';
    } else {
      let html = '';
      for (const [id, peer] of this.peers) {
        const info = this.nodeInfo.get(id) || {};
        const hostname = info.hostname || id;
        const platform = info.platform || '';
        const offset = peer.clockOffset;
        const offsetStr = Math.abs(offset) < 1 ? '' : (offset > 0 ? `+${offset.toFixed(0)}ms` : `${offset.toFixed(0)}ms`);
        const rttStr = peer.rtt ? `${peer.rtt}ms` : '';

        let statusClass = '';
        let statusLabel = '';
        if (peer.dead) {
          statusClass = 'dead';
          statusLabel = '离线';
        } else if (peer.relayOnly) {
          statusClass = 'relay';
          statusLabel = 'Relay';
        } else {
          statusClass = '';
          statusLabel = 'P2P';
        }

        const metaParts = [statusLabel];
        if (platform) metaParts.push(platform);
        if (rttStr) metaParts.push(rttStr);
        if (offsetStr) metaParts.push(`Δ${offsetStr}`);

        html += `<div class="node-item">
          <div class="node-dot${statusClass ? ' ' + statusClass : ''}" title="${statusLabel}"></div>
          <div class="node-info">
            <div class="node-name${peer.dead ? ' node-name-dead' : ''}" title="${id}">${hostname}</div>
            <div class="node-meta">${metaParts.join(' · ')}</div>
            ${(!peer.dead && peer.connected) ? `<div class="node-actions"><button class="btn-mini" data-collect="${id}">🚨 紧急采集</button></div>` : ''}
          </div>
        </div>`;
      }
      container.innerHTML = html;

      container.querySelectorAll('[data-collect]').forEach(btn => {
        btn.addEventListener('click', (e) => {
          e.stopPropagation();
          const nodeId = btn.getAttribute('data-collect');
          this.startEmergencyCollect(nodeId);
        });
      });
    }

    const queryBtn = document.getElementById('queryBtn');
    const hasLive = Array.from(this.peers.values()).some(p => p.connected && !p.dead);
    queryBtn.disabled = !hasLive;
  }

  _updateSourceFilter() {
    const select = document.getElementById('sourceFilter');
    const current = select.value;
    const options = ['<option value="">全部</option>'];
    for (const source of Array.from(this.sources).sort()) {
      options.push(`<option value="${source}"${source === current ? ' selected' : ''}>${source}</option>`);
    }
    select.innerHTML = options.join('');
  }

  _updateStats() {
    document.getElementById('statTotal').textContent = this.stats.total.toLocaleString();
    document.getElementById('statError').textContent = this.stats.ERROR.toLocaleString();
    document.getElementById('statWarn').textContent = this.stats.WARN.toLocaleString();
    document.getElementById('statInfo').textContent = this.stats.INFO.toLocaleString();
    document.getElementById('statDebug').textContent = this.stats.DEBUG.toLocaleString();
    document.getElementById('logCount').textContent = `${this.filteredLogs.length.toLocaleString()} 条日志`;
  }

  updateStatus(cls, text) {
    const badge = document.getElementById('connectionStatus');
    badge.className = `status-badge ${cls}`;
    badge.textContent = text;
  }

  disconnect() {
    this.stopHeartbeat();
    this.stopQuery();
    for (const [, peer] of this.peers) {
      peer.close();
    }
    this.peers.clear();
    this.nodeInfo.clear();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.signalUrl = '';
    this.wsReconnectAttempts = 0;
    this.updateStatus('', '未连接');
    this._updateNodeList();
  }

  switchTab(tab) {
    this.currentTab = tab;
    document.querySelectorAll('.tab-btn').forEach(b => {
      b.classList.toggle('active', b.dataset.tab === tab);
    });
    document.querySelectorAll('.tab-panel').forEach(p => {
      p.classList.toggle('active', p.id === `tab-${tab}`);
    });
    const showLogs = tab === 'logs';
    ['logsToolbar', 'logsToolbar2', 'logsToolbar3'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.style.display = showLogs ? '' : 'none';
    });
    document.getElementById('queryBtn').style.display = showLogs ? '' : 'none';
    document.getElementById('stopBtn').style.display = showLogs ? '' : 'none';
    if (tab === 'dashboard') {
      setTimeout(() => this._initCharts(), 50);
    }
  }

  _initCharts() {
    if (!window.echarts) return;
    const ids = ['chartErrors', 'chartThroughput', 'chartLevelDist'];
    for (const id of ids) {
      const el = document.getElementById(id);
      if (!el || this.charts[id]) continue;
      this.charts[id] = echarts.init(el, 'dark');
    }
    this._renderDashboard();
    window.addEventListener('resize', () => {
      for (const c of Object.values(this.charts)) {
        try { c.resize(); } catch {}
      }
    });
  }

  _handleHealthStats(fromId, msg) {
    const entry = {
      ts: Date.now(),
      perLevel: msg.perLevel || {},
      buckets: msg.buckets || []
    };
    const history = this.healthData.get(fromId) || [];
    history.push(entry);
    while (history.length > 60) history.shift();
    this.healthData.set(fromId, history);

    if (this.currentTab === 'dashboard') {
      this._renderDashboard();
    }
  }

  _renderDashboard() {
    const totalStats = { DEBUG: 0, INFO: 0, WARN: 0, ERROR: 0, total: 0 };
    const nodeSummaries = [];
    let allBuckets = [];
    for (const [nodeId, history] of this.healthData) {
      const latest = history[history.length - 1];
      if (!latest) continue;
      const info = this.nodeInfo.get(nodeId) || {};
      const hostname = info.hostname || nodeId.slice(0, 20);
      const peer = this.peers.get(nodeId);
      const dead = peer ? peer.dead : false;
      for (const k of Object.keys(totalStats)) {
        totalStats[k] += latest.perLevel[k] || 0;
      }
      const errRate = latest.perLevel.total > 0
        ? ((latest.perLevel.ERROR / latest.perLevel.total) * 100).toFixed(1) + '%'
        : '0%';
      nodeSummaries.push({ nodeId, hostname, dead, latest, errRate });
      for (const b of latest.buckets) {
        allBuckets.push({ ...b, nodeId, hostname });
      }
    }

    const sumEl = document.getElementById('dashboardSummary');
    if (sumEl) {
      const cards = [
        { label: '在线节点', value: nodeSummaries.filter(n => !n.dead).length, sub: `共 ${nodeSummaries.length} 个`, color: 'var(--accent)' },
        { label: '日志总数（1分钟）', value: totalStats.total.toLocaleString(), sub: '各节点汇总', color: 'var(--text-primary)' },
        { label: 'ERROR', value: totalStats.ERROR.toLocaleString(), sub: '近 1 分钟', color: 'var(--danger)' },
        { label: 'WARN', value: totalStats.WARN.toLocaleString(), sub: '近 1 分钟', color: 'var(--warning)' }
      ];
      sumEl.innerHTML = cards.map(c => `
        <div class="summary-card">
          <div class="summary-label">${c.label}</div>
          <div class="summary-value" style="color:${c.color}">${c.value}</div>
          <div class="summary-sub">${c.sub}</div>
        </div>
      `).join('');
    }

    if (!this.charts.chartErrors || !this.charts.chartThroughput || !this.charts.chartLevelDist) return;

    const bucketMap = new Map();
    const nodeNames = new Map();
    for (const b of allBuckets) {
      const key = b.ts;
      if (!bucketMap.has(key)) bucketMap.set(key, {});
      const m = bucketMap.get(key);
      nodeNames.set(b.nodeId, b.hostname);
      if (!m[b.nodeId]) m[b.nodeId] = { ERROR: 0, total: 0 };
      m[b.nodeId].ERROR += b.ERROR || 0;
      m[b.nodeId].total += b.total || 0;
    }
    const times = Array.from(bucketMap.keys()).sort((a, b) => a - b);
    const timeLabels = times.map(t => new Date(t).toLocaleTimeString('zh-CN', { hour12: false }));

    const errSeries = [];
    const tpSeries = [];
    const colors = ['#58a6ff', '#3fb950', '#f85149', '#d29922', '#bc8cff', '#f778ba'];
    let ci = 0;
    for (const [nodeId, hostname] of nodeNames) {
      const color = colors[ci++ % colors.length];
      errSeries.push({
        name: hostname,
        type: 'line',
        smooth: true,
        showSymbol: false,
        lineStyle: { color, width: 2 },
        itemStyle: { color },
        data: times.map(t => {
          const b = bucketMap.get(t)[nodeId];
          if (!b || !b.total) return 0;
          return +((b.ERROR / b.total) * 100).toFixed(2);
        })
      });
      tpSeries.push({
        name: hostname,
        type: 'line',
        smooth: true,
        showSymbol: false,
        lineStyle: { color, width: 2 },
        itemStyle: { color },
        areaStyle: { opacity: 0.1 },
        data: times.map(t => bucketMap.get(t)[nodeId]?.total || 0)
      });
    }

    this.charts.chartErrors.setOption({
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      legend: { top: 0, textStyle: { color: '#8b949e', fontSize: 11 } },
      grid: { left: 40, right: 16, top: 30, bottom: 28 },
      xAxis: { type: 'category', data: timeLabels, axisLabel: { color: '#8b949e', fontSize: 10 } },
      yAxis: { type: 'value', name: '错误率 %', axisLabel: { color: '#8b949e', fontSize: 10 }, nameTextStyle: { color: '#8b949e', fontSize: 10 } },
      series: errSeries
    }, true);

    this.charts.chartThroughput.setOption({
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      legend: { top: 0, textStyle: { color: '#8b949e', fontSize: 11 } },
      grid: { left: 40, right: 16, top: 30, bottom: 28 },
      xAxis: { type: 'category', data: timeLabels, axisLabel: { color: '#8b949e', fontSize: 10 } },
      yAxis: { type: 'value', name: '条/10秒', axisLabel: { color: '#8b949e', fontSize: 10 }, nameTextStyle: { color: '#8b949e', fontSize: 10 } },
      series: tpSeries
    }, true);

    this.charts.chartLevelDist.setOption({
      backgroundColor: 'transparent',
      tooltip: { trigger: 'item' },
      legend: { top: 0, textStyle: { color: '#8b949e', fontSize: 11 } },
      series: [{
        type: 'pie',
        radius: ['40%', '70%'],
        avoidLabelOverlap: true,
        label: { color: '#e6edf3', fontSize: 12 },
        data: [
          { value: totalStats.ERROR, name: 'ERROR', itemStyle: { color: '#f85149' } },
          { value: totalStats.WARN, name: 'WARN', itemStyle: { color: '#d29922' } },
          { value: totalStats.INFO, name: 'INFO', itemStyle: { color: '#58a6ff' } },
          { value: totalStats.DEBUG, name: 'DEBUG', itemStyle: { color: '#8b949e' } }
        ]
      }]
    }, true);
  }

  startEmergencyCollect(nodeId) {
    const info = this.nodeInfo.get(nodeId) || {};
    const hostname = info.hostname || nodeId;
    const collectId = `collect-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    const state = {
      nodeId, hostname, collectId,
      totalSize: 0, chunkCount: 0,
      receivedChunks: new Map(),
      startedAt: Date.now(),
      finished: false
    };
    this.activeCollects.set(collectId, state);

    document.getElementById('collectTargetName').textContent = hostname;
    document.getElementById('collectFill').style.width = '0%';
    document.getElementById('collectProgressText').textContent = '等待响应...';
    document.getElementById('collectStatus').textContent = '正在发送采集指令...';
    document.getElementById('downloadCollectBtn').disabled = true;
    document.getElementById('collectModal').style.display = 'flex';

    const req = { type: MSG_TYPE.COLLECT_REQ, collectId, sourceId: this.nodeId, targetId: nodeId, ts: Date.now() };
    const peer = this.peers.get(nodeId);
    if (peer && peer.connected && !peer.dead) {
      peer.send(req);
    } else {
      this.sendRelay(nodeId, req);
    }
  }

  _handleCollectAck(msg) {
    const st = this.activeCollects.get(msg.collectId);
    if (!st) return;
    st.totalSize = msg.totalSize;
    st.chunkCount = msg.chunkCount;
    document.getElementById('collectStatus').textContent =
      `接收分片：0 / ${msg.chunkCount}（${(msg.totalSize/1024).toFixed(1)} KB）`;
    document.getElementById('collectProgressText').textContent = `0 / ${msg.chunkCount}`;
  }

  _handleCollectData(msg) {
    const st = this.activeCollects.get(msg.collectId);
    if (!st || st.finished) return;
    st.receivedChunks.set(msg.chunkIndex, msg.data);
    const got = st.receivedChunks.size;
    const total = st.chunkCount;
    const pct = total ? (got / total * 100).toFixed(1) : 0;
    document.getElementById('collectFill').style.width = `${pct}%`;
    document.getElementById('collectProgressText').textContent = `${got} / ${total}  (${pct}%)`;
    document.getElementById('collectStatus').textContent =
      `接收分片：${got} / ${total} · ${(st.totalSize/1024).toFixed(1)} KB`;
  }

  _handleCollectEnd(msg) {
    const st = this.activeCollects.get(msg.collectId);
    if (!st) return;
    const ordered = [];
    for (let i = 0; i < st.chunkCount; i++) {
      const c = st.receivedChunks.get(i);
      if (c == null) {
        document.getElementById('collectStatus').textContent = `❌ 缺失分片 #${i}，传输失败`;
        return;
      }
      ordered.push(c);
    }
    try {
      const b64 = ordered.join('');
      const bin = atob(b64);
      const bytes = new Uint8Array(bin.length);
      for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
      const blob = new Blob([bytes], { type: 'text/plain;charset=utf-8' });
      st.blob = blob;
      st.fileName = `emergency-log-${st.hostname}-${new Date().toISOString().replace(/[:.]/g, '-')}.txt`;
      st.finished = true;

      document.getElementById('collectFill').style.width = '100%';
      document.getElementById('collectProgressText').textContent = `✓ 完成 (${(blob.size/1024).toFixed(1)} KB)`;
      document.getElementById('collectStatus').textContent =
        `✓ 采集完成，共 ${st.chunkCount} 个分片，${(blob.size/1024).toFixed(1)} KB`;
      document.getElementById('downloadCollectBtn').disabled = false;
      document.getElementById('downloadCollectBtn').onclick = () => this._downloadCollect(msg.collectId);
    } catch (e) {
      document.getElementById('collectStatus').textContent = `❌ 重组失败: ${e.message}`;
    }
  }

  _downloadCollect(collectId) {
    const st = this.activeCollects.get(collectId);
    if (!st || !st.blob) return;
    const url = URL.createObjectURL(st.blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = st.fileName;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }
}

class VirtualScroll {
  constructor(viewportEl, contentEl, rowHeight) {
    this.viewport = viewportEl;
    this.content = contentEl;
    this.rowHeight = rowHeight;
    this.items = [];
    this.buffer = 20;
    this.prevStart = -1;
    this.prevEnd = -1;
    this.renderedRows = new Map();

    this.viewport.addEventListener('scroll', () => this._onScroll());
  }

  setItems(items) {
    this.items = items;
    this.prevStart = -1;
    this.prevEnd = -1;
    this.content.style.height = `${items.length * this.rowHeight}px`;
    this._clearRenderedRows();
    this._render();
  }

  appendItem(item) {
    this.items.push(item);
    this.content.style.height = `${this.items.length * this.rowHeight}px`;
    const scrollTop = this.viewport.scrollTop;
    const viewportH = this.viewport.clientHeight;
    const endIdx = Math.ceil((scrollTop + viewportH) / this.rowHeight) + this.buffer;

    if (this.items.length - 1 <= endIdx) {
      this._renderRow(this.items.length - 1);
    }
  }

  _onScroll() {
    requestAnimationFrame(() => this._render());
  }

  _render() {
    const scrollTop = this.viewport.scrollTop;
    const viewportH = this.viewport.clientHeight;

    const startIdx = Math.max(0, Math.floor(scrollTop / this.rowHeight) - this.buffer);
    const endIdx = Math.min(this.items.length, Math.ceil((scrollTop + viewportH) / this.rowHeight) + this.buffer);

    if (startIdx === this.prevStart && endIdx === this.prevEnd) return;

    const newRendered = new Map();

    for (let i = startIdx; i < endIdx; i++) {
      if (this.renderedRows.has(i)) {
        newRendered.set(i, this.renderedRows.get(i));
        this.renderedRows.delete(i);
      } else {
        const row = this._createRow(i, this.items[i]);
        this.content.appendChild(row);
        newRendered.set(i, row);
      }
    }

    for (const [, row] of this.renderedRows) {
      if (row.parentNode) row.parentNode.removeChild(row);
    }

    this.renderedRows = newRendered;
    this.prevStart = startIdx;
    this.prevEnd = endIdx;
  }

  _createRow(index, entry) {
    const row = document.createElement('div');
    row.className = `log-row level-${(entry.level || 'info').toLowerCase()}`;
    row.style.top = `${index * this.rowHeight}px`;
    row.style.height = `${this.rowHeight}px`;

    const displayTs = entry.calibratedTimestamp || entry.timestamp;
    const dt = new Date(displayTs);
    const time = dt.toLocaleTimeString('zh-CN', {
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      fractionalSecondDigits: 3
    });

    row.innerHTML = `
      <span class="log-cell log-cell-time">${time}</span>
      <span class="log-cell log-cell-level level-${(entry.level || 'info').toLowerCase()}">${entry.level || 'INFO'}</span>
      <span class="log-cell log-cell-source">${this._esc(entry.source || '-')}</span>
      <span class="log-cell log-cell-node">${this._esc(entry.hostname || entry.nodeId || '-')}</span>
      <span class="log-cell log-cell-message">${this._esc(entry.message || '')}</span>
    `;

    row.addEventListener('click', () => {
      const detail = document.getElementById('logDetail');
      const detailContent = document.getElementById('logDetailContent');
      detail.style.display = 'flex';
      detailContent.textContent = JSON.stringify(entry, null, 2);
    });

    return row;
  }

  _esc(str) {
    const div = document.createElement('span');
    div.textContent = str;
    return div.innerHTML;
  }

  _clearRenderedRows() {
    for (const [, row] of this.renderedRows) {
      if (row.parentNode) row.parentNode.removeChild(row);
    }
    this.renderedRows.clear();
    this.content.innerHTML = '';
  }

  scrollToBottom() {
    this.viewport.scrollTop = this.content.scrollHeight;
  }
}

const grid = new LogGrid();

document.addEventListener('DOMContentLoaded', () => {
  const viewport = document.getElementById('logViewport');
  const content = document.getElementById('logContent');

  grid.virtualScroll = new VirtualScroll(viewport, content, 28);

  document.getElementById('connectBtn').addEventListener('click', () => {
    const btn = document.getElementById('connectBtn');
    if (grid.ws && grid.ws.readyState === 1) {
      grid.disconnect();
      btn.textContent = '连接网格';
      btn.classList.remove('btn-danger');
      btn.classList.add('btn-primary');
      document.getElementById('stopBtn').disabled = true;
    } else {
      const url = document.getElementById('signalUrl').value;
      grid.connect(url);
      btn.textContent = '断开连接';
      btn.classList.remove('btn-primary');
      btn.classList.add('btn-danger');
    }
  });

  document.getElementById('queryBtn').addEventListener('click', () => {
    const level = document.getElementById('levelFilter').value;
    const keyword = document.getElementById('keywordFilter').value;
    const source = document.getElementById('sourceFilter').value;

    const filters = {};
    if (level) filters.minLevel = level;
    if (keyword) filters.keyword = keyword;
    if (source) filters.source = source;

    grid.startQuery(filters);
    document.getElementById('stopBtn').disabled = false;
    document.getElementById('queryBtn').disabled = true;
  });

  document.getElementById('stopBtn').addEventListener('click', () => {
    grid.stopQuery();
    document.getElementById('stopBtn').disabled = true;
    const hasLive = Array.from(grid.peers.values()).some(p => p.connected && !p.dead);
    document.getElementById('queryBtn').disabled = !hasLive;
  });

  document.getElementById('autoScroll').addEventListener('change', (evt) => {
    grid.autoScroll = evt.target.checked;
  });

  document.getElementById('levelFilter').addEventListener('change', () => {
    grid.applyLocalFilter();
  });

  document.getElementById('keywordFilter').addEventListener('input', (() => {
    let timer = null;
    return (evt) => {
      clearTimeout(timer);
      timer = setTimeout(() => grid.applyLocalFilter(), 300);
    };
  })());

  document.getElementById('sourceFilter').addEventListener('change', () => {
    grid.applyLocalFilter();
  });

  document.getElementById('closeDetailBtn').addEventListener('click', () => {
    document.getElementById('logDetail').style.display = 'none';
  });

  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => grid.switchTab(btn.dataset.tab));
  });

  const closeCollect = () => {
    document.getElementById('collectModal').style.display = 'none';
  };
  document.getElementById('closeCollectBtn').addEventListener('click', closeCollect);
  document.getElementById('cancelCollectBtn').addEventListener('click', closeCollect);
  document.getElementById('collectModal').addEventListener('click', (e) => {
    if (e.target.id === 'collectModal') closeCollect();
  });
});

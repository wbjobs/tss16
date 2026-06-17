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
  PONG: 'pong'
};

const LOG_LEVELS = ['DEBUG', 'INFO', 'WARN', 'ERROR'];
const LOG_LEVEL_ORDER = { DEBUG: 0, INFO: 1, WARN: 2, ERROR: 3 };

function encode(msg) {
  return JSON.stringify(msg);
}

function decode(data) {
  try {
    return JSON.parse(data);
  } catch {
    return null;
  }
}

function makeRegister(nodeId, info) {
  return { type: MSG_TYPE.REGISTER, nodeId, info };
}

function makeNodes(nodes) {
  return { type: MSG_TYPE.NODES, nodes };
}

function makeNodeJoined(nodeId, info) {
  return { type: MSG_TYPE.NODE_JOINED, nodeId, info };
}

function makeNodeLeft(nodeId) {
  return { type: MSG_TYPE.NODE_LEFT, nodeId };
}

function makeSignal(targetId, signal) {
  return { type: MSG_TYPE.SIGNAL, targetId, signal };
}

function makeRelay(targetId, data) {
  return { type: MSG_TYPE.RELAY, targetId, data };
}

function makeQuery(queryId, sourceId, filters, ttl) {
  return { type: MSG_TYPE.QUERY, queryId, sourceId, filters, ttl: ttl || 5 };
}

function makeQueryCancel(queryId, sourceId) {
  return { type: MSG_TYPE.QUERY_CANCEL, queryId, sourceId };
}

function makeLogStream(queryId, sourceId, entry) {
  return { type: MSG_TYPE.LOG_STREAM, queryId, sourceId, entry };
}

function makeLogStreamEnd(queryId, sourceId) {
  return { type: MSG_TYPE.LOG_STREAM_END, queryId, sourceId };
}

function makePing() {
  return { type: MSG_TYPE.PING, ts: Date.now() };
}

function makePong(ts) {
  return { type: MSG_TYPE.PONG, ts };
}

module.exports = {
  MSG_TYPE,
  LOG_LEVELS,
  LOG_LEVEL_ORDER,
  encode,
  decode,
  makeRegister,
  makeNodes,
  makeNodeJoined,
  makeNodeLeft,
  makeSignal,
  makeRelay,
  makeQuery,
  makeQueryCancel,
  makeLogStream,
  makeLogStreamEnd,
  makePing,
  makePong
};

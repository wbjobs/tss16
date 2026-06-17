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

function makePing(ts) {
  return { type: MSG_TYPE.PING, ts: ts || Date.now(), t1: Date.now() };
}

function makePong(ts, t1) {
  return { type: MSG_TYPE.PONG, ts, t1: t1 || 0, t2: Date.now() };
}

function makeCollectReq(collectId, sourceId, targetId) {
  return { type: MSG_TYPE.COLLECT_REQ, collectId, sourceId, targetId, ts: Date.now() };
}

function makeCollectAck(collectId, sourceId, targetId, totalSize, chunkCount) {
  return { type: MSG_TYPE.COLLECT_ACK, collectId, sourceId, targetId, totalSize, chunkCount, ts: Date.now() };
}

function makeCollectData(collectId, sourceId, targetId, chunkIndex, chunkCount, data) {
  return { type: MSG_TYPE.COLLECT_DATA, collectId, sourceId, targetId, chunkIndex, chunkCount, data, ts: Date.now() };
}

function makeCollectEnd(collectId, sourceId, targetId, chunkCount, checksum) {
  return { type: MSG_TYPE.COLLECT_END, collectId, sourceId, targetId, chunkCount, checksum, ts: Date.now() };
}

function makeHealthStats(sourceId, windowMs, perLevel, buckets) {
  return { type: MSG_TYPE.HEALTH_STATS, sourceId, windowMs, perLevel, buckets, ts: Date.now() };
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
  makePong,
  makeCollectReq,
  makeCollectAck,
  makeCollectData,
  makeCollectEnd,
  makeHealthStats
};
